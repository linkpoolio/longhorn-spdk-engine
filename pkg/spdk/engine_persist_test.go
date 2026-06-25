package spdk

import (
	"fmt"
	"os"

	. "gopkg.in/check.v1"

	"github.com/longhorn/longhorn-spdk-engine/pkg/types"
)

// An engine record must round-trip DialedAddress and Transport per replica:
// after an IM/spdk_tgt restart the reconnect and validation paths compare the
// attached bdev against dialAddress(), so a restore that drops these fields
// fails dial-address validation for every replica whose dial fell back to the
// TCP listener at primary+1. Restoring Transport also keeps recoverEngines'
// tcp->rdma upgrade from misclassifying entries with empty Transport.
func (s *TestSuite) TestEngineRecordRoundTripPreservesDialedAddressAndTransport(c *C) {
	fmt.Println("Testing engine record round-trip preserves DialedAddress/Transport")

	metadataDir := c.MkDir()

	e := NewEngine("engine-rt", "vol-rt", types.FrontendSPDKTCPNvmf, 1024, NvmfTransportRDMA, make(chan interface{}, 1), defaultTestSnapshotMaxCount)
	e.metadataDir = metadataDir
	e.RaidBdevUUID = "raid-uuid-1"
	e.deltaBitmapEnabled = false
	e.QosLimits = QosLimits{RwMBPerSec: 100}
	e.ReplicaStatusMap = map[string]*EngineReplicaStatus{
		// Fallback-dialed replica: canonical primary differs from the +1 TCP dial.
		"r-fallback": {
			Address:       "10.10.3.19:28923",
			DialedAddress: "10.10.3.19:28924",
			Transport:     NvmfTransportTCP,
			BdevName:      "r-fallback-1n1",
			Mode:          types.ModeRW,
		},
		// RDMA-dialed replica: dialed the primary as-is.
		"r-rdma": {
			Address:       "10.10.3.20:28923",
			DialedAddress: "10.10.3.20:28923",
			Transport:     NvmfTransportRDMA,
			BdevName:      "r-rdma-1n1",
			Mode:          types.ModeRW,
		},
	}

	c.Assert(saveEngineRecord(metadataDir, e), IsNil)

	records, err := loadEngineRecords(metadataDir)
	c.Assert(err, IsNil)
	rec := records["engine-rt"]
	c.Assert(rec, NotNil)

	restored := NewEngine(rec.Name, rec.VolumeName, rec.Frontend, rec.SpecSize, rec.ReplicaTransport, make(chan interface{}, 1), 0)
	restored.restoreFromRecord(rec)

	fb := restored.ReplicaStatusMap["r-fallback"]
	c.Assert(fb, NotNil)
	c.Check(fb.Address, Equals, "10.10.3.19:28923")
	c.Check(fb.DialedAddress, Equals, "10.10.3.19:28924")
	c.Check(fb.Transport, Equals, NvmfTransportTCP)
	c.Check(fb.BdevName, Equals, "r-fallback-1n1")
	c.Check(fb.Mode, Equals, types.ModeRW)
	// dialAddress() must keep pointing at the +1 fallback after restore.
	c.Check(fb.dialAddress(), Equals, "10.10.3.19:28924")

	rd := restored.ReplicaStatusMap["r-rdma"]
	c.Assert(rd, NotNil)
	c.Check(rd.DialedAddress, Equals, "10.10.3.20:28923")
	c.Check(rd.Transport, Equals, NvmfTransportRDMA)

	// The explicitly persisted false must survive the round trip even though
	// the engine default is true.
	c.Check(restored.deltaBitmapEnabled, Equals, false)
	c.Check(restored.QosLimits, Equals, QosLimits{RwMBPerSec: 100})
}

// A record written by an older build has no deltaBitmapEnabled field at all.
// Restoring it must keep the default the engine would have chosen on fresh
// create (defaultRaidDeltaBitmapEnabled(), true unless overridden via env)
// instead of flipping the raid delta-bitmap flag to false across reconstruct.
func (s *TestSuite) TestEngineRecordDeltaBitmapAbsentKeepsDefault(c *C) {
	fmt.Println("Testing engine record without deltaBitmapEnabled keeps the fresh-create default")

	metadataDir := c.MkDir()

	legacyJSON := []byte(`{
  "name": "engine-legacy",
  "volumeName": "vol-legacy",
  "frontend": "spdk-tcp-nvmf",
  "specSize": 1048576
}`)
	dir := engineRecordDir(metadataDir, "engine-legacy")
	c.Assert(os.MkdirAll(dir, 0o750), IsNil)
	c.Assert(os.WriteFile(engineRecordPath(metadataDir, "engine-legacy"), legacyJSON, 0o640), IsNil)

	records, err := loadEngineRecords(metadataDir)
	c.Assert(err, IsNil)
	rec := records["engine-legacy"]
	c.Assert(rec, NotNil)
	c.Assert(rec.DeltaBitmapEnabled, IsNil)

	e := NewEngine(rec.Name, rec.VolumeName, rec.Frontend, rec.SpecSize, NvmfTransportTCP, make(chan interface{}, 1), 0)
	defaultValue := e.deltaBitmapEnabled // what fresh create would have chosen
	e.restoreFromRecord(rec)
	c.Check(e.deltaBitmapEnabled, Equals, defaultValue)

	// An explicit false in the record still wins over the default.
	explicitFalse := false
	rec.DeltaBitmapEnabled = &explicitFalse
	e2 := NewEngine(rec.Name, rec.VolumeName, rec.Frontend, rec.SpecSize, NvmfTransportTCP, make(chan interface{}, 1), 0)
	e2.restoreFromRecord(rec)
	c.Check(e2.deltaBitmapEnabled, Equals, false)
}
