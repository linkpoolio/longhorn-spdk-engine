package spdk

import (
	"encoding/base64"
	"time"

	"github.com/cockroachdb/errors"

	spdktypes "github.com/longhorn/go-spdk-helper/pkg/spdk/types"

	"github.com/longhorn/longhorn-spdk-engine/pkg/types"
)

// ReplicaDirtyBitmap captures the dirty-region bitmap of a replica base bdev
// at the moment it disconnected from a raid1 engine. The bitmap is SPDK's
// native base64 bit-array; one bit per region of RegionSize bytes.
//
// Scope note (reset stack): bitmaps live only in the running engine's memory.
// There is no engine record persistence on reset/main, so a captured bitmap
// does not survive an IM restart — after a restart the reconnecting replica
// falls back to the ordinary (checksum-driven) rebuild path.
type ReplicaDirtyBitmap struct {
	// Data is the base64-encoded spdk_bit_array returned by
	// bdev_raid_get_base_bdev_delta_bitmap. Bit i (LSB-first within each
	// byte, matching spdk_bit_array_to_base64_string) marks region i dirty.
	Data string `json:"data"`
	// RegionSize is the per-bit region size in bytes, equal to
	// raid.optimal_io_boundary × raid.blocklen at capture time.
	RegionSize uint64 `json:"regionSize"`
	// BdevName records which base bdev the bitmap covers. A subsequent
	// reconnect that presents a different base bdev name should fall back
	// to full resync rather than apply a bitmap keyed on the old one.
	BdevName string `json:"bdevName"`
	// CapturedAt is the wall-clock time of capture. Used for telemetry and
	// to age out stale bitmaps (a long-absent replica is indistinguishable
	// from a fresh one).
	CapturedAt time.Time `json:"capturedAt"`
}

// ClusterList converts the bitmap into a sorted, de-duplicated list of dirty
// lvstore cluster indexes: for every dirty region, every cluster overlapping
// [i*RegionSize, (i+1)*RegionSize) is included. The result is the input the
// range shallow copy path (BdevLvolStartRangeShallowCopy) expects.
func (bm *ReplicaDirtyBitmap) ClusterList(clusterSize uint64) ([]uint64, error) {
	if bm == nil {
		return nil, errors.New("nil dirty bitmap")
	}
	if clusterSize == 0 || bm.RegionSize == 0 {
		return nil, errors.Errorf("invalid cluster size %d or region size %d for dirty bitmap cluster conversion", clusterSize, bm.RegionSize)
	}
	raw, err := base64.StdEncoding.DecodeString(bm.Data)
	if err != nil {
		return nil, errors.Wrap(err, "failed to decode dirty bitmap payload")
	}

	clusters := make([]uint64, 0)
	appended := false
	var last uint64
	for i := uint64(0); i < uint64(len(raw))*8; i++ {
		if raw[i/8]&(1<<(i%8)) == 0 {
			continue
		}
		first := i * bm.RegionSize / clusterSize
		lastOfRegion := ((i+1)*bm.RegionSize - 1) / clusterSize
		for c := first; c <= lastOfRegion; c++ {
			if appended && c <= last {
				continue
			}
			clusters = append(clusters, c)
			last = c
			appended = true
		}
	}
	return clusters, nil
}

// deltaBitmapRaidClient is the narrow slice of the SPDK client used by the
// bitmap capture sequence; *spdkclient.Client satisfies it and tests can
// substitute a fake.
type deltaBitmapRaidClient interface {
	BdevRaidStopBaseBdevDeltaBitmap(baseBdevName string) (bool, error)
	BdevRaidGetBaseBdevDeltaBitmap(baseBdevName string) (*spdktypes.BdevRaidBaseBdevDeltaBitmapResponse, error)
	BdevRaidClearBaseBdevFaultyState(baseBdevName string) (bool, error)
}

// captureBitmapsForFaultedReplicasNoLock walks ReplicaStatusMap and for
// every replica that transitioned from a non-ERR mode to ERR during the
// current ValidateAndUpdate pass, captures the SPDK-side dirty bitmap into
// the engine's in-memory ReplicaDirtyBitmaps so a subsequent replica-add of
// the same (reused) replica can rebuild incrementally.
//
// The SPDK raid1 module auto-transitions a base bdev's per-channel state
// NONE → FAULTY as soon as a write arrives while the base channel is absent
// (raid1_handle_faulty_base_bdev). This function then calls:
//   - bdev_raid_stop_base_bdev_delta_bitmap  (aggregates, state → FAULTY_STOPPED)
//   - bdev_raid_get_base_bdev_delta_bitmap   (returns base64 bitmap + region size)
//   - bdev_raid_clear_base_bdev_faulty_state (frees SPDK tracking state)
//
// Any failure is logged and the replica falls back to full-resync semantics.
// A -ENODEV from stop() means no writes landed during the disconnect window —
// there's nothing to rebuild, so we skip cleanly.
func (e *Engine) captureBitmapsForFaultedReplicasNoLock(spdkClient deltaBitmapRaidClient, previousModes map[string]types.Mode) {
	if !e.deltaBitmapEnabled {
		return
	}
	for name, status := range e.ReplicaStatusMap {
		if status == nil {
			continue
		}
		if status.Mode != types.ModeERR {
			continue
		}
		prev, hadPrev := previousModes[name]
		if !hadPrev || prev == types.ModeERR {
			// Already ERR before this pass — bitmap was either captured
			// earlier or the transition pre-dates our tracking. Skip.
			continue
		}
		if status.BdevName == "" {
			e.log.Warnf("Cannot capture delta bitmap for replica %s: no bdev name recorded", name)
			continue
		}
		if err := e.captureBitmapForReplicaNoLock(spdkClient, name, status.BdevName); err != nil {
			e.log.WithError(err).Warnf("Failed to capture delta bitmap for replica %s (bdev %s); reconnect will fall back to full resync", name, status.BdevName)
		}
	}
}

// captureBitmapForReplicaNoLock executes the three-step SPDK sequence for a
// single base bdev and records the result in memory. Callers must already
// hold e's lock so the write to ReplicaDirtyBitmaps is safe.
func (e *Engine) captureBitmapForReplicaNoLock(spdkClient deltaBitmapRaidClient, replicaName, bdevName string) error {
	if _, err := spdkClient.BdevRaidStopBaseBdevDeltaBitmap(bdevName); err != nil {
		// -ENODEV means the base bdev was not recorded as faulty by SPDK,
		// which in turn means no writes arrived during the disconnect. The
		// replica is bit-identical to the healthy side; full-resync path
		// will no-op anyway.
		return errors.Wrap(err, "stop")
	}

	resp, err := spdkClient.BdevRaidGetBaseBdevDeltaBitmap(bdevName)
	if err != nil {
		return errors.Wrap(err, "get")
	}
	if resp == nil || resp.RegionSize == 0 {
		return errors.New("bdev raid get returned empty bitmap response")
	}

	if e.ReplicaDirtyBitmaps == nil {
		e.ReplicaDirtyBitmaps = map[string]*ReplicaDirtyBitmap{}
	}
	e.ReplicaDirtyBitmaps[replicaName] = &ReplicaDirtyBitmap{
		Data:       resp.DeltaBitmap,
		RegionSize: resp.RegionSize,
		BdevName:   bdevName,
		CapturedAt: time.Now().UTC(),
	}

	if _, err := spdkClient.BdevRaidClearBaseBdevFaultyState(bdevName); err != nil {
		// The bitmap is already recorded — failing to clear is not fatal.
		// SPDK's 600s auto-clear poller will reap the in-memory state.
		e.log.WithError(err).Warnf("Failed to clear faulty state for bdev %s after bitmap capture; SPDK will auto-clear after 600s", bdevName)
	}

	e.log.Infof("Captured delta bitmap for replica %s (bdev=%s regionSize=%d bytes)",
		replicaName, bdevName, resp.RegionSize)
	return nil
}

// snapshotReplicaModesNoLock captures the current per-replica mode before a
// validation pass, so captureBitmapsForFaultedReplicasNoLock can detect which
// replicas transitioned RW->ERR during this tick. Caller must hold e.Lock.
func (e *Engine) snapshotReplicaModesNoLock() map[string]types.Mode {
	prev := make(map[string]types.Mode, len(e.ReplicaStatusMap))
	for name, status := range e.ReplicaStatusMap {
		if status == nil {
			continue
		}
		prev[name] = status.Mode
	}
	return prev
}

// clearReplicaDirtyBitmapNoLock drops the captured bitmap for a replica.
// Caller must hold e.Lock.
func (e *Engine) clearReplicaDirtyBitmapNoLock(replicaName, reason string) {
	if _, ok := e.ReplicaDirtyBitmaps[replicaName]; !ok {
		return
	}
	delete(e.ReplicaDirtyBitmaps, replicaName)
	e.log.Infof("Cleared captured delta bitmap for replica %s: %s", replicaName, reason)
}

// clearAllReplicaDirtyBitmapsNoLock drops every captured bitmap. Caller must
// hold e.Lock.
func (e *Engine) clearAllReplicaDirtyBitmapsNoLock() {
	if len(e.ReplicaDirtyBitmaps) == 0 {
		return
	}
	e.log.Infof("Clearing %d captured delta bitmap(s) on engine deletion", len(e.ReplicaDirtyBitmaps))
	e.ReplicaDirtyBitmaps = nil
}
