package spdk

import (
	"encoding/json"
	"os"
	"path/filepath"

	"github.com/sirupsen/logrus"

	safelog "github.com/longhorn/longhorn-spdk-engine/pkg/log"
	"github.com/longhorn/longhorn-spdk-engine/pkg/types"

	. "gopkg.in/check.v1"
)

// newTestEngine constructs an Engine directly (no SPDK client) for unit
// testing persistence + restore. NewEngine wires other state we do not need
// here (e.g. replicaAdder); this helper keeps the tests focused on the record
// shape and restore semantics.
func newTestEngine(name, volume string) *Engine {
	return &Engine{
		Name:             name,
		VolumeName:       volume,
		Frontend:         types.FrontendSPDKTCPBlockdev,
		SpecSize:         uint64(1048576),
		ReplicaTransport: NvmfTransportTCP,
		ReplicaStatusMap: map[string]*EngineReplicaStatus{},
		RaidBdevUUID:     "raid-uuid-abc",
		NvmeTcpTarget: &NvmeTcpTarget{
			IP:        "10.0.0.1",
			Port:      int32(3000),
			Nqn:       "nqn.example:" + name,
			Nguid:     "nguid-xyz",
			ANAState:  NvmeTCPANAStateOptimized,
			Transport: NvmfTransportTCP,
		},
		log: safelog.NewSafeLogger(logrus.StandardLogger().WithField("engineName", name)),
	}
}

func (s *TestSuite) TestSaveAndLoadEngineRecord(c *C) {
	tmpDir := c.MkDir()

	e := newTestEngine("engine-a", "vol-a")
	e.deltaBitmapEnabled = true
	e.ReplicaStatusMap["replica-1"] = &EngineReplicaStatus{Address: "10.0.0.2:4420", BdevName: "bdev-1", Mode: "RW"}
	e.ReplicaStatusMap["replica-2"] = &EngineReplicaStatus{Address: "10.0.0.3:4421", BdevName: "bdev-2", Mode: "WO"}

	c.Assert(saveEngineRecord(tmpDir, e), IsNil)

	records, err := loadEngineRecords(tmpDir)
	c.Assert(err, IsNil)
	c.Assert(len(records), Equals, 1)

	rec := records["engine-a"]
	c.Assert(rec, NotNil)
	c.Assert(rec.Name, Equals, "engine-a")
	c.Assert(rec.VolumeName, Equals, "vol-a")
	c.Assert(rec.Frontend, Equals, types.FrontendSPDKTCPBlockdev)
	c.Assert(rec.SpecSize, Equals, uint64(1048576))
	c.Assert(rec.RaidBdevUUID, Equals, "raid-uuid-abc")
	c.Assert(rec.ReplicaTransport, Equals, NvmfTransportTCP)
	c.Assert(len(rec.ReplicaStatusMap), Equals, 2)
	c.Assert(rec.ReplicaStatusMap["replica-1"].BdevName, Equals, "bdev-1")
	c.Assert(rec.ReplicaStatusMap["replica-2"].Mode, Equals, types.Mode("WO"))
	c.Assert(rec.NvmeTcpTarget, NotNil)
	c.Assert(rec.NvmeTcpTarget.IP, Equals, "10.0.0.1")
	c.Assert(rec.NvmeTcpTarget.Port, Equals, int32(3000))
	c.Assert(rec.NvmeTcpTarget.Nqn, Equals, "nqn.example:engine-a")
	c.Assert(rec.DeltaBitmapEnabled, Equals, true)
}

func (s *TestSuite) TestSaveAndLoadEngineRecordPreservesDeltaBitmapDisabled(c *C) {
	tmpDir := c.MkDir()

	// Explicit false round-trips cleanly even with omitempty encoding:
	// restoreFromRecord needs to observe the persisted value, not a zero-value default.
	e := newTestEngine("engine-off", "vol-off")
	e.deltaBitmapEnabled = false

	c.Assert(saveEngineRecord(tmpDir, e), IsNil)
	records, err := loadEngineRecords(tmpDir)
	c.Assert(err, IsNil)
	rec := records["engine-off"]
	c.Assert(rec, NotNil)
	c.Assert(rec.DeltaBitmapEnabled, Equals, false)

	fresh := &Engine{log: e.log}
	fresh.restoreFromRecord(rec)
	c.Assert(fresh.deltaBitmapEnabled, Equals, false)
}

func (s *TestSuite) TestSaveEngineRecordIsAtomic(c *C) {
	tmpDir := c.MkDir()
	e := newTestEngine("engine-atomic", "vol-atomic")

	c.Assert(saveEngineRecord(tmpDir, e), IsNil)

	// No lingering .tmp sibling after a successful save.
	entries, err := os.ReadDir(engineRecordDir(tmpDir, "engine-atomic"))
	c.Assert(err, IsNil)
	for _, entry := range entries {
		if filepath.Ext(entry.Name()) == ".tmp" {
			c.Fatalf("found stray temp file after save: %s", entry.Name())
		}
	}
}

func (s *TestSuite) TestSaveEngineRecordEmptyMetadataDirNoOp(c *C) {
	e := newTestEngine("engine-noop", "vol-noop")
	// Empty metadataDir should short-circuit without writing or erroring.
	c.Assert(saveEngineRecord("", e), IsNil)

	records, err := loadEngineRecords("")
	c.Assert(err, IsNil)
	c.Assert(records, IsNil)
}

func (s *TestSuite) TestRemoveEngineRecord(c *C) {
	tmpDir := c.MkDir()
	e := newTestEngine("engine-rm", "vol-rm")

	c.Assert(saveEngineRecord(tmpDir, e), IsNil)

	records, err := loadEngineRecords(tmpDir)
	c.Assert(err, IsNil)
	c.Assert(len(records), Equals, 1)

	c.Assert(removeEngineRecord(tmpDir, "engine-rm"), IsNil)

	// Directory should be gone.
	_, err = os.Stat(engineRecordDir(tmpDir, "engine-rm"))
	c.Assert(os.IsNotExist(err), Equals, true)

	// Loading should now return zero records.
	records, err = loadEngineRecords(tmpDir)
	c.Assert(err, IsNil)
	c.Assert(len(records), Equals, 0)
}

func (s *TestSuite) TestRemoveEngineRecordIdempotent(c *C) {
	tmpDir := c.MkDir()
	// Removing a record that was never written must be a no-op, not an error.
	c.Assert(removeEngineRecord(tmpDir, "engine-never-saved"), IsNil)
}

func (s *TestSuite) TestLoadEngineRecordsSkipsCorruptJSON(c *C) {
	tmpDir := c.MkDir()

	// Write an unparsable engine.json.
	badName := "engine-bad"
	badDir := engineRecordDir(tmpDir, badName)
	c.Assert(os.MkdirAll(badDir, 0o750), IsNil)
	c.Assert(os.WriteFile(engineRecordPath(tmpDir, badName), []byte("not-json"), 0o640), IsNil)

	// Also write a valid record to ensure one bad record doesn't poison the rest.
	good := newTestEngine("engine-good", "vol-good")
	c.Assert(saveEngineRecord(tmpDir, good), IsNil)

	records, err := loadEngineRecords(tmpDir)
	c.Assert(err, IsNil)
	c.Assert(len(records), Equals, 1)
	c.Assert(records["engine-good"], NotNil)
	c.Assert(records["engine-bad"], IsNil)

	// Corrupt record directory should have been removed by the loader.
	_, err = os.Stat(badDir)
	c.Assert(os.IsNotExist(err), Equals, true)
}

func (s *TestSuite) TestLoadEngineRecordsSkipsIncompleteRecord(c *C) {
	tmpDir := c.MkDir()

	incompleteName := "engine-incomplete"
	dir := engineRecordDir(tmpDir, incompleteName)
	c.Assert(os.MkdirAll(dir, 0o750), IsNil)
	// Empty Name / VolumeName — structurally valid JSON but semantically invalid.
	data, err := json.Marshal(&EngineRecord{})
	c.Assert(err, IsNil)
	c.Assert(os.WriteFile(engineRecordPath(tmpDir, incompleteName), data, 0o640), IsNil)

	records, err := loadEngineRecords(tmpDir)
	c.Assert(err, IsNil)
	c.Assert(len(records), Equals, 0)

	_, err = os.Stat(dir)
	c.Assert(os.IsNotExist(err), Equals, true)
}

func (s *TestSuite) TestLoadEngineRecordsEmptyDir(c *C) {
	tmpDir := c.MkDir()
	// No records written yet — loader must return (nil, nil), not fail.
	records, err := loadEngineRecords(tmpDir)
	c.Assert(err, IsNil)
	c.Assert(len(records), Equals, 0)
}

func (s *TestSuite) TestEngineRestoreFromRecord(c *C) {
	tmpDir := c.MkDir()

	// Stage a record on disk.
	original := newTestEngine("engine-x", "vol-x")
	original.deltaBitmapEnabled = true
	original.ReplicaStatusMap["r1"] = &EngineReplicaStatus{Address: "10.0.0.5:4420", BdevName: "bdev-r1", Mode: "RW"}
	c.Assert(saveEngineRecord(tmpDir, original), IsNil)

	records, err := loadEngineRecords(tmpDir)
	c.Assert(err, IsNil)
	rec := records["engine-x"]
	c.Assert(rec, NotNil)

	// Fresh engine, no state.
	fresh := &Engine{log: safelog.NewSafeLogger(logrus.StandardLogger().WithField("engineName", "engine-x"))}
	fresh.restoreFromRecord(rec)

	c.Assert(fresh.Name, Equals, "engine-x")
	c.Assert(fresh.VolumeName, Equals, "vol-x")
	c.Assert(fresh.Frontend, Equals, types.FrontendSPDKTCPBlockdev)
	c.Assert(fresh.SpecSize, Equals, uint64(1048576))
	c.Assert(fresh.RaidBdevUUID, Equals, "raid-uuid-abc")
	c.Assert(fresh.ReplicaTransport, Equals, NvmfTransportTCP)
	c.Assert(len(fresh.ReplicaStatusMap), Equals, 1)
	c.Assert(fresh.ReplicaStatusMap["r1"].BdevName, Equals, "bdev-r1")
	c.Assert(fresh.NvmeTcpTarget, NotNil)
	c.Assert(fresh.NvmeTcpTarget.Port, Equals, int32(3000))
	c.Assert(fresh.deltaBitmapEnabled, Equals, true)

	// Mutating the restored map must not reach back into the record — verifies
	// the deep copy in restoreFromRecord. If the record is reloaded on a
	// subsequent restart, it must still reflect what was written.
	fresh.ReplicaStatusMap["r1"].Mode = "ERR"
	reloaded, err := loadEngineRecords(tmpDir)
	c.Assert(err, IsNil)
	c.Assert(reloaded["engine-x"].ReplicaStatusMap["r1"].Mode, Equals, types.Mode("RW"))
}

func (s *TestSuite) TestEngineRestoreFromRecordNilSafe(c *C) {
	e := &Engine{log: safelog.NewSafeLogger(logrus.StandardLogger().WithField("engineName", "engine-nil"))}
	// Must not panic on nil record.
	e.restoreFromRecord(nil)
	c.Assert(e.Name, Equals, "")
}

func (s *TestSuite) TestSaveEngineRecordMultipleRoundTrips(c *C) {
	tmpDir := c.MkDir()

	e := newTestEngine("engine-rt", "vol-rt")

	// Initial save with no replicas.
	c.Assert(saveEngineRecord(tmpDir, e), IsNil)
	records, err := loadEngineRecords(tmpDir)
	c.Assert(err, IsNil)
	c.Assert(len(records["engine-rt"].ReplicaStatusMap), Equals, 0)

	// Add a replica and re-save.
	e.ReplicaStatusMap["replica-a"] = &EngineReplicaStatus{Address: "10.0.0.10:4420", BdevName: "bdev-a", Mode: "RW"}
	c.Assert(saveEngineRecord(tmpDir, e), IsNil)

	records, err = loadEngineRecords(tmpDir)
	c.Assert(err, IsNil)
	c.Assert(len(records["engine-rt"].ReplicaStatusMap), Equals, 1)

	// Remove replica and re-save.
	delete(e.ReplicaStatusMap, "replica-a")
	c.Assert(saveEngineRecord(tmpDir, e), IsNil)

	records, err = loadEngineRecords(tmpDir)
	c.Assert(err, IsNil)
	c.Assert(len(records["engine-rt"].ReplicaStatusMap), Equals, 0)
}

