package spdk

import (
	"time"

	"github.com/longhorn/longhorn-spdk-engine/pkg/types"

	. "gopkg.in/check.v1"
)

func (s *TestSuite) TestSnapshotReplicaModesNoLock(c *C) {
	e := newTestEngine("engine-snap", "vol-snap")
	e.ReplicaStatusMap["r1"] = &EngineReplicaStatus{Mode: types.ModeRW, BdevName: "bdev-1"}
	e.ReplicaStatusMap["r2"] = &EngineReplicaStatus{Mode: types.ModeWO, BdevName: "bdev-2"}
	e.ReplicaStatusMap["r3"] = nil // skipped

	snap := e.snapshotReplicaModesNoLock()
	c.Assert(snap["r1"], Equals, types.ModeRW)
	c.Assert(snap["r2"], Equals, types.ModeWO)
	_, has := snap["r3"]
	c.Assert(has, Equals, false)
}

func (s *TestSuite) TestCaptureBitmapSkippedWhenDisabled(c *C) {
	// With deltaBitmapEnabled=false the capture path must not attempt RPCs
	// even when a replica transitions to ERR. Passing a nil spdk client
	// proves we don't touch it.
	e := newTestEngine("engine-off", "vol-off")
	e.deltaBitmapEnabled = false
	e.ReplicaStatusMap["r1"] = &EngineReplicaStatus{Mode: types.ModeERR, BdevName: "bdev-1"}

	e.captureBitmapsForFaultedReplicasNoLock(nil, map[string]types.Mode{"r1": types.ModeRW})
	c.Assert(len(e.ReplicaDirtyBitmaps), Equals, 0)
}

func (s *TestSuite) TestCaptureBitmapSkippedWhenAlreadyERR(c *C) {
	// If a replica was already ERR on the previous pass, the bitmap was
	// (or wasn't) captured then — don't double-capture this pass.
	e := newTestEngine("engine-prev-err", "vol-prev-err")
	e.deltaBitmapEnabled = true
	e.ReplicaStatusMap["r1"] = &EngineReplicaStatus{Mode: types.ModeERR, BdevName: "bdev-1"}

	e.captureBitmapsForFaultedReplicasNoLock(nil, map[string]types.Mode{"r1": types.ModeERR})
	c.Assert(len(e.ReplicaDirtyBitmaps), Equals, 0)
}

func (s *TestSuite) TestCaptureBitmapSkippedWhenStillHealthy(c *C) {
	e := newTestEngine("engine-ok", "vol-ok")
	e.deltaBitmapEnabled = true
	e.ReplicaStatusMap["r1"] = &EngineReplicaStatus{Mode: types.ModeRW, BdevName: "bdev-1"}

	e.captureBitmapsForFaultedReplicasNoLock(nil, map[string]types.Mode{"r1": types.ModeRW})
	c.Assert(len(e.ReplicaDirtyBitmaps), Equals, 0)
}

func (s *TestSuite) TestReplicaDirtyBitmapPersistenceRoundTrip(c *C) {
	tmpDir := c.MkDir()

	e := newTestEngine("engine-bm", "vol-bm")
	e.deltaBitmapEnabled = true
	e.ReplicaDirtyBitmaps = map[string]*ReplicaDirtyBitmap{
		"r1": {Data: "AAAA", RegionSize: 4096, BdevName: "bdev-1", CapturedAt: time.Date(2026, 4, 23, 10, 0, 0, 0, time.UTC)},
	}

	c.Assert(saveEngineRecord(tmpDir, e), IsNil)

	records, err := loadEngineRecords(tmpDir)
	c.Assert(err, IsNil)
	rec := records["engine-bm"]
	c.Assert(rec, NotNil)
	c.Assert(len(rec.ReplicaDirtyBitmaps), Equals, 1)
	c.Assert(rec.ReplicaDirtyBitmaps["r1"].Data, Equals, "AAAA")
	c.Assert(rec.ReplicaDirtyBitmaps["r1"].RegionSize, Equals, uint64(4096))
	c.Assert(rec.ReplicaDirtyBitmaps["r1"].BdevName, Equals, "bdev-1")
	c.Assert(rec.ReplicaDirtyBitmaps["r1"].CapturedAt.Equal(time.Date(2026, 4, 23, 10, 0, 0, 0, time.UTC)), Equals, true)

	// Restore + verify deep copy: mutating restored map must not leak into record on disk.
	fresh := &Engine{log: e.log}
	fresh.restoreFromRecord(rec)
	c.Assert(len(fresh.ReplicaDirtyBitmaps), Equals, 1)
	c.Assert(fresh.ReplicaDirtyBitmaps["r1"].Data, Equals, "AAAA")

	fresh.ReplicaDirtyBitmaps["r1"].Data = "MUTATED"
	reloaded, err := loadEngineRecords(tmpDir)
	c.Assert(err, IsNil)
	c.Assert(reloaded["engine-bm"].ReplicaDirtyBitmaps["r1"].Data, Equals, "AAAA")
}

func (s *TestSuite) TestReplicaDirtyBitmapOmittedWhenEmpty(c *C) {
	tmpDir := c.MkDir()

	e := newTestEngine("engine-nobm", "vol-nobm")
	c.Assert(saveEngineRecord(tmpDir, e), IsNil)

	records, err := loadEngineRecords(tmpDir)
	c.Assert(err, IsNil)
	rec := records["engine-nobm"]
	c.Assert(rec, NotNil)
	c.Assert(len(rec.ReplicaDirtyBitmaps), Equals, 0)
}
