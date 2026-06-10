package spdk

import (
	"fmt"

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

	e := NewEngine("engine-rt", "vol-rt", types.FrontendSPDKTCPNvmf, 1024, NvmfTransportRDMA, make(chan interface{}, 1), defaultTestSnapshotMaxCount, nil)
	e.metadataDir = metadataDir
	e.RaidBdevUUID = "raid-uuid-1"
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

	restored := NewEngine(rec.Name, rec.VolumeName, rec.Frontend, rec.SpecSize, rec.ReplicaTransport, make(chan interface{}, 1), 0, nil)
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

	c.Check(restored.RaidBdevUUID, Equals, "raid-uuid-1")
	c.Check(restored.ReplicaTransport, Equals, NvmfTransportRDMA)
}
