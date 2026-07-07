package spdk

import (
	"encoding/base64"
	"fmt"

	"github.com/cockroachdb/errors"

	. "gopkg.in/check.v1"

	spdktypes "github.com/longhorn/go-spdk-helper/pkg/spdk/types"

	"github.com/longhorn/longhorn-spdk-engine/pkg/types"
)

// fakeDeltaBitmapRaidClient implements deltaBitmapRaidClient for capture
// tests, recording the call sequence and simulating per-step failures.
type fakeDeltaBitmapRaidClient struct {
	calls []string

	stopErr  error
	getErr   error
	clearErr error

	bitmap     string
	regionSize uint64
}

func (f *fakeDeltaBitmapRaidClient) BdevRaidStopBaseBdevDeltaBitmap(baseBdevName string) (bool, error) {
	f.calls = append(f.calls, "stop:"+baseBdevName)
	if f.stopErr != nil {
		return false, f.stopErr
	}
	return true, nil
}

func (f *fakeDeltaBitmapRaidClient) BdevRaidGetBaseBdevDeltaBitmap(baseBdevName string) (*spdktypes.BdevRaidBaseBdevDeltaBitmapResponse, error) {
	f.calls = append(f.calls, "get:"+baseBdevName)
	if f.getErr != nil {
		return nil, f.getErr
	}
	return &spdktypes.BdevRaidBaseBdevDeltaBitmapResponse{
		DeltaBitmap: f.bitmap,
		RegionSize:  f.regionSize,
	}, nil
}

func (f *fakeDeltaBitmapRaidClient) BdevRaidClearBaseBdevFaultyState(baseBdevName string) (bool, error) {
	f.calls = append(f.calls, "clear:"+baseBdevName)
	if f.clearErr != nil {
		return false, f.clearErr
	}
	return true, nil
}

func newBitmapTestEngine(name string) *Engine {
	return NewEngine(name, "vol-"+name, types.FrontendSPDKTCPNvmf, 1024, NvmfTransportTCP, make(chan interface{}, 1), defaultTestSnapshotMaxCount, nil)
}

// captureBitmapsForFaultedReplicasNoLock must only act on replicas that
// transitioned into ERR during the current validation pass: replicas that
// were already ERR (bitmap captured earlier or transition pre-dates
// tracking), replicas without a previous mode, healthy replicas, and
// replicas without a recorded bdev name are all skipped. None of the skip
// paths touch SPDK, which the nil client below enforces (a wrongly-entered
// capture would nil-deref).
func (s *TestSuite) TestCaptureBitmapsForFaultedReplicasGating(c *C) {
	fmt.Println("Testing captureBitmapsForFaultedReplicasNoLock gating")

	e := newBitmapTestEngine("engine-bm")
	e.ReplicaStatusMap = map[string]*EngineReplicaStatus{
		"r-healthy":     {Mode: types.ModeRW, BdevName: "r-healthy-1n1"},
		"r-already-err": {Mode: types.ModeERR, BdevName: "r-already-err-1n1"},
		"r-no-prev":     {Mode: types.ModeERR, BdevName: "r-no-prev-1n1"},
		"r-no-bdev":     {Mode: types.ModeERR},
		"r-nil":         nil,
	}

	previousModes := map[string]types.Mode{
		"r-healthy":     types.ModeRW,
		"r-already-err": types.ModeERR,
		// r-no-prev intentionally absent
		"r-no-bdev": types.ModeRW,
	}

	// Disabled flag: full no-op regardless of transitions.
	e.deltaBitmapEnabled = false
	e.captureBitmapsForFaultedReplicasNoLock(nil, previousModes)
	c.Check(len(e.ReplicaDirtyBitmaps), Equals, 0)

	// Enabled: every entry above hits a skip path, so the nil SPDK client is
	// never dereferenced and no bitmap is recorded.
	e.deltaBitmapEnabled = true
	e.captureBitmapsForFaultedReplicasNoLock(nil, previousModes)
	c.Check(len(e.ReplicaDirtyBitmaps), Equals, 0)
}

// A replica that transitioned RW→ERR this pass must have its bitmap captured
// via the stop → get → clear sequence and recorded in ReplicaDirtyBitmaps.
func (s *TestSuite) TestCaptureBitmapOnErrTransition(c *C) {
	fmt.Println("Testing dirty bitmap capture on RW->ERR transition")

	e := newBitmapTestEngine("engine-bm-cap")
	e.deltaBitmapEnabled = true
	e.ReplicaStatusMap = map[string]*EngineReplicaStatus{
		"r-faulted": {Mode: types.ModeERR, BdevName: "r-faulted-1n1"},
		"r-healthy": {Mode: types.ModeRW, BdevName: "r-healthy-1n1"},
	}
	previousModes := map[string]types.Mode{
		"r-faulted": types.ModeRW,
		"r-healthy": types.ModeRW,
	}

	fake := &fakeDeltaBitmapRaidClient{
		bitmap:     base64.StdEncoding.EncodeToString([]byte{0x05}),
		regionSize: defaultClusterSize,
	}
	e.captureBitmapsForFaultedReplicasNoLock(fake, previousModes)

	c.Assert(fake.calls, DeepEquals, []string{"stop:r-faulted-1n1", "get:r-faulted-1n1", "clear:r-faulted-1n1"})
	c.Assert(e.ReplicaDirtyBitmaps["r-faulted"], NotNil)
	c.Check(e.ReplicaDirtyBitmaps["r-faulted"].BdevName, Equals, "r-faulted-1n1")
	c.Check(e.ReplicaDirtyBitmaps["r-faulted"].RegionSize, Equals, uint64(defaultClusterSize))
	c.Check(e.ReplicaDirtyBitmaps["r-faulted"].Data, Equals, fake.bitmap)
	c.Check(e.ReplicaDirtyBitmaps["r-faulted"].CapturedAt.IsZero(), Equals, false)
	c.Check(len(e.ReplicaDirtyBitmaps), Equals, 1)

	// A second pass with the replica already ERR must not re-capture.
	fake.calls = nil
	e.captureBitmapsForFaultedReplicasNoLock(fake, e.snapshotReplicaModesNoLock())
	c.Check(len(fake.calls), Equals, 0)
}

// Failures in the capture sequence fall back to full-resync semantics: a
// stop or get failure records nothing; a clear failure is non-fatal because
// the bitmap is already recorded (SPDK auto-clears after 600s).
func (s *TestSuite) TestCaptureBitmapFailureModes(c *C) {
	fmt.Println("Testing dirty bitmap capture failure modes")

	e := newBitmapTestEngine("engine-bm-fail")
	e.deltaBitmapEnabled = true

	// stop fails (e.g. -ENODEV: no writes during the disconnect window)
	fake := &fakeDeltaBitmapRaidClient{stopErr: errors.New("No such device")}
	err := e.captureBitmapForReplicaNoLock(fake, "r-1", "r-1n1")
	c.Check(err, NotNil)
	c.Check(len(e.ReplicaDirtyBitmaps), Equals, 0)
	c.Check(fake.calls, DeepEquals, []string{"stop:r-1n1"})

	// get fails
	fake = &fakeDeltaBitmapRaidClient{getErr: errors.New("boom")}
	err = e.captureBitmapForReplicaNoLock(fake, "r-1", "r-1n1")
	c.Check(err, NotNil)
	c.Check(len(e.ReplicaDirtyBitmaps), Equals, 0)

	// empty response (regionSize 0) is rejected
	fake = &fakeDeltaBitmapRaidClient{bitmap: "AA==", regionSize: 0}
	err = e.captureBitmapForReplicaNoLock(fake, "r-1", "r-1n1")
	c.Check(err, NotNil)
	c.Check(len(e.ReplicaDirtyBitmaps), Equals, 0)

	// clear failing is non-fatal: bitmap stays recorded
	fake = &fakeDeltaBitmapRaidClient{bitmap: "AQ==", regionSize: 4096, clearErr: errors.New("busy")}
	err = e.captureBitmapForReplicaNoLock(fake, "r-1", "r-1n1")
	c.Check(err, IsNil)
	c.Assert(e.ReplicaDirtyBitmaps["r-1"], NotNil)
}

// snapshotReplicaModesNoLock captures the per-replica mode for transition
// detection and must skip nil entries.
func (s *TestSuite) TestSnapshotReplicaModesNoLock(c *C) {
	fmt.Println("Testing snapshotReplicaModesNoLock")

	e := newBitmapTestEngine("engine-bm2")
	e.ReplicaStatusMap = map[string]*EngineReplicaStatus{
		"r-1":   {Mode: types.ModeRW},
		"r-2":   {Mode: types.ModeERR},
		"r-nil": nil,
	}

	prev := e.snapshotReplicaModesNoLock()
	c.Check(prev, DeepEquals, map[string]types.Mode{
		"r-1": types.ModeRW,
		"r-2": types.ModeERR,
	})
}

// ClusterList decodes the SPDK bit array (LSB-first within each byte, one
// bit per RegionSize bytes) into sorted, de-duplicated lvstore cluster
// indexes covering every dirty region.
func (s *TestSuite) TestReplicaDirtyBitmapClusterList(c *C) {
	fmt.Println("Testing ReplicaDirtyBitmap.ClusterList conversion")

	const clusterSize = uint64(32 * 1024 * 1024)

	// region == cluster: bits 0 and 2 of byte 0, bit 0 of byte 1 (region 8)
	bm := &ReplicaDirtyBitmap{
		Data:       base64.StdEncoding.EncodeToString([]byte{0x05, 0x01}),
		RegionSize: clusterSize,
	}
	clusters, err := bm.ClusterList(clusterSize)
	c.Assert(err, IsNil)
	c.Check(clusters, DeepEquals, []uint64{0, 2, 8})

	// region = 2 clusters: region 1 expands to clusters 2,3
	bm = &ReplicaDirtyBitmap{
		Data:       base64.StdEncoding.EncodeToString([]byte{0x02}),
		RegionSize: 2 * clusterSize,
	}
	clusters, err = bm.ClusterList(clusterSize)
	c.Assert(err, IsNil)
	c.Check(clusters, DeepEquals, []uint64{2, 3})

	// region = 1/4 cluster: regions 0..3 all map to cluster 0, dedup;
	// region 4 maps to cluster 1.
	bm = &ReplicaDirtyBitmap{
		Data:       base64.StdEncoding.EncodeToString([]byte{0x1F}),
		RegionSize: clusterSize / 4,
	}
	clusters, err = bm.ClusterList(clusterSize)
	c.Assert(err, IsNil)
	c.Check(clusters, DeepEquals, []uint64{0, 1})

	// empty bitmap: no clusters
	bm = &ReplicaDirtyBitmap{
		Data:       base64.StdEncoding.EncodeToString([]byte{0x00, 0x00}),
		RegionSize: clusterSize,
	}
	clusters, err = bm.ClusterList(clusterSize)
	c.Assert(err, IsNil)
	c.Check(len(clusters), Equals, 0)

	// error cases
	_, err = (&ReplicaDirtyBitmap{Data: "AQ==", RegionSize: 0}).ClusterList(clusterSize)
	c.Check(err, NotNil)
	_, err = (&ReplicaDirtyBitmap{Data: "AQ==", RegionSize: clusterSize}).ClusterList(0)
	c.Check(err, NotNil)
	_, err = (&ReplicaDirtyBitmap{Data: "not-base64!!", RegionSize: clusterSize}).ClusterList(clusterSize)
	c.Check(err, NotNil)
	var nilBm *ReplicaDirtyBitmap
	_, err = nilBm.ClusterList(clusterSize)
	c.Check(err, NotNil)
}

// The bitmap lifecycle: captured entries are consumed-and-cleared by a
// successful rebuild of the same replica name and dropped wholesale on
// engine delete.
func (s *TestSuite) TestReplicaDirtyBitmapClearHelpers(c *C) {
	fmt.Println("Testing dirty bitmap clear helpers")

	e := newBitmapTestEngine("engine-bm3")
	e.ReplicaDirtyBitmaps = map[string]*ReplicaDirtyBitmap{
		"r-1": {Data: "AQ==", RegionSize: 4096, BdevName: "r-1n1"},
		"r-2": {Data: "AQ==", RegionSize: 4096, BdevName: "r-2n1"},
	}

	// Clearing an unknown replica is a no-op.
	e.clearReplicaDirtyBitmapNoLock("r-unknown", "test")
	c.Check(len(e.ReplicaDirtyBitmaps), Equals, 2)

	e.clearReplicaDirtyBitmapNoLock("r-1", "replica rebuilt successfully")
	c.Check(e.ReplicaDirtyBitmaps["r-1"], IsNil)
	c.Check(e.ReplicaDirtyBitmaps["r-2"], NotNil)

	e.clearAllReplicaDirtyBitmapsNoLock()
	c.Check(len(e.ReplicaDirtyBitmaps), Equals, 0)
}
