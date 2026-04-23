package spdk

import (
	"time"

	"github.com/cockroachdb/errors"

	spdkclient "github.com/longhorn/go-spdk-helper/pkg/spdk/client"

	"github.com/longhorn/longhorn-spdk-engine/pkg/types"
)

// ReplicaDirtyBitmap captures the dirty-region bitmap of a replica base bdev
// at the moment it disconnected from a raid1 engine. The bitmap is SPDK's
// native base64 bit-array; one bit per region of RegionSize bytes. On
// replica reconnect the engine replays these regions via shallow-copy
// instead of a full resync.
type ReplicaDirtyBitmap struct {
	// Data is the base64-encoded spdk_bit_array returned by
	// bdev_raid_get_base_bdev_delta_bitmap.
	Data string `json:"data"`
	// RegionSize is the per-bit region size in bytes, equal to
	// raid.optimal_io_boundary × raid.blocklen at capture time.
	RegionSize uint64 `json:"regionSize"`
	// BdevName records which base bdev the bitmap covers. A subsequent
	// reconnect that presents a different base bdev name should fall back
	// to full resync rather than apply a bitmap keyed on the old one.
	BdevName string `json:"bdevName"`
	// CapturedAt is the wall-clock time of capture. Used for telemetry and
	// to age out stale bitmaps (the engine should discard any bitmap older
	// than a configurable threshold — a long-absent replica is indistinguishable
	// from a fresh one).
	CapturedAt time.Time `json:"capturedAt"`
}

// captureBitmapsForFaultedReplicasNoLock walks ReplicaStatusMap and for
// every replica that transitioned from a non-ERR mode to ERR during the
// current ValidateAndUpdate pass, captures + persists the SPDK-side dirty
// bitmap so a subsequent replica-add can rebuild incrementally.
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
func (e *Engine) captureBitmapsForFaultedReplicasNoLock(spdkClient *spdkclient.Client, previousModes map[string]types.Mode) {
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
// single base bdev and persists the result. Callers must already hold e's
// lock so the write to ReplicaDirtyBitmaps is safe.
func (e *Engine) captureBitmapForReplicaNoLock(spdkClient *spdkclient.Client, replicaName, bdevName string) error {
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
		// Bitmap is safely persisted — failing to clear is not fatal.
		// SPDK's 600s auto-clear poller will reap the in-memory state.
		e.log.WithError(err).Warnf("Failed to clear faulty state for bdev %s after bitmap capture; SPDK will auto-clear after 600s", bdevName)
	}

	if saveErr := saveEngineRecord(e.metadataDir, e); saveErr != nil {
		e.log.WithError(saveErr).Warnf("Failed to persist engine record for %s after bitmap capture of replica %s", e.Name, replicaName)
	}

	e.log.Infof("Captured delta bitmap for replica %s (bdev=%s regionSize=%d bytes)",
		replicaName, bdevName, resp.RegionSize)
	return nil
}
