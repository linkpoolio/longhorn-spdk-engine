package spdk

import (
	"context"
	"fmt"
	"os"

	. "gopkg.in/check.v1"

	spdktypes "github.com/longhorn/go-spdk-helper/pkg/spdk/types"
	helpertypes "github.com/longhorn/go-spdk-helper/pkg/types"

	"github.com/longhorn/longhorn-spdk-engine/pkg/types"
)

// The replica reconciler must heal exactly the documented condition — the
// in-memory replica says it should be exposed but SPDK shows no NVMe-oF
// listener — and must not let SPDK probe errors count toward the heal
// threshold (a failed probe says nothing about the listener and Heal would be
// just as likely to fail against the same busy target).
func (s *TestSuite) TestDeriveReplicaHealSignal(c *C) {
	fmt.Println("Testing deriveReplicaHealSignal classification")

	head := &Lvol{Name: "r-1"}

	cases := []struct {
		name            string
		shouldBeExposed bool
		derived         *Replica
		probeErr        error
		want            replicaHealSignal
	}{
		{
			name:            "exposed + listener -> healthy",
			shouldBeExposed: true,
			derived:         &Replica{State: types.InstanceStateRunning, Head: head, IsExposed: true},
			want:            replicaHealSignalHealthy,
		},
		{
			name:            "exposed + no listener -> heal candidate",
			shouldBeExposed: true,
			derived:         &Replica{State: types.InstanceStateStopped, Head: head, IsExposed: false},
			want:            replicaHealSignalCandidate,
		},
		{
			name:            "not exposed + no listener -> fine",
			shouldBeExposed: false,
			derived:         &Replica{State: types.InstanceStateStopped, Head: head, IsExposed: false},
			want:            replicaHealSignalNone,
		},
		{
			name:            "probe error -> skip",
			shouldBeExposed: true,
			derived:         nil,
			probeErr:        fmt.Errorf("spdk busy"),
			want:            replicaHealSignalSkipProbe,
		},
		{
			name:            "derived Error state (structural/probe problem) -> skip",
			shouldBeExposed: true,
			derived:         &Replica{State: types.InstanceStateError, Head: head},
			want:            replicaHealSignalSkipProbe,
		},
		{
			name:            "exposed but head lvol gone -> not healable here",
			shouldBeExposed: true,
			derived:         &Replica{State: types.InstanceStateStopped, Head: nil},
			want:            replicaHealSignalNone,
		},
	}

	for _, tc := range cases {
		c.Check(deriveReplicaHealSignal(tc.shouldBeExposed, tc.derived, tc.probeErr), Equals, tc.want, Commentf("case %q", tc.name))
	}
}

// Heal must mirror Create's expose sequence: same NQN, the same generated
// NGUID Create uses (generateNGUID(name), not ""), the record's IP/primary
// port, and on RDMA nodes the secondary TCP fallback listener at primary+1.
func (s *TestSuite) TestReplicaHealExposeParams(c *C) {
	fmt.Println("Testing Replica.healExposeParams mirrors Create's expose sequence")

	record := &ReplicaRecord{Name: "r-heal", IP: "10.0.0.9", PortStart: 21000, PortEnd: 21015}

	rRDMA := NewReplica(context.Background(), "r-heal", "lvs", "lvs-uuid", 1024, false, NvmfTransportRDMA, make(chan interface{}, 1))
	p := rRDMA.healExposeParams(record)
	c.Check(p.Nqn, Equals, helpertypes.GetNQN("r-heal"))
	c.Check(p.Nguid, Equals, generateNGUID("r-heal")) // what Create passes, never ""
	c.Check(p.Nguid, Not(Equals), "")
	c.Check(p.IP, Equals, "10.0.0.9")
	c.Check(p.Port, Equals, int32(21000))
	c.Check(p.Transport, Equals, NvmfTransportRDMA)
	c.Check(p.NeedTCPFallback, Equals, true)

	rTCP := NewReplica(context.Background(), "r-heal", "lvs", "lvs-uuid", 1024, false, NvmfTransportTCP, make(chan interface{}, 1))
	pTCP := rTCP.healExposeParams(record)
	c.Check(pTCP.Transport, Equals, NvmfTransportTCP)
	c.Check(pTCP.NeedTCPFallback, Equals, false)
}

// A cleanly-stopped replica (Delete with cleanupRequired=false) must keep its
// persisted record: ReplicaGet/ReplicaList are record-driven and
// BuildReplicaFromRecord derives the legitimate Stopped state from it. Only a
// real cleanup delete removes the record.
func (s *TestSuite) TestReplicaRecordSurvivesCleanStop(c *C) {
	fmt.Println("Testing replica record survives Delete(cleanupRequired=false)")

	metadataDir := c.MkDir()
	r := NewReplica(context.Background(), "r-stop", "lvs", "lvs-uuid", 1024, false, NvmfTransportTCP, make(chan interface{}, 1))
	r.metadataDir = metadataDir
	r.IP = "10.0.0.1"
	r.PortStart, r.PortEnd = 21000, 21015
	c.Assert(saveReplicaRecord(metadataDir, r), IsNil)

	// Clean stop: the record must survive.
	r.finishDeleteRecord(false)
	rec, err := loadReplicaRecord(metadataDir, "r-stop")
	c.Assert(err, IsNil)
	c.Assert(rec, NotNil)
	c.Check(rec.PortStart, Equals, int32(21000))

	// Real cleanup: the record must go.
	r.finishDeleteRecord(true)
	rec, err = loadReplicaRecord(metadataDir, "r-stop")
	c.Assert(err, IsNil)
	c.Check(rec, IsNil)
	_, statErr := os.Stat(replicaRecordPath(metadataDir, "r-stop"))
	c.Check(os.IsNotExist(statErr), Equals, true)
}

func headBdevFixture(replicaName, lvsName, lvsUUID string) spdktypes.BdevInfo {
	bdev := spdktypes.BdevInfo{}
	bdev.Name = replicaName
	bdev.Aliases = []string{lvsName + "/" + replicaName}
	bdev.ProductName = spdktypes.BdevProductNameLvol
	bdev.UUID = "head-uuid"
	bdev.BlockSize = 512
	bdev.NumBlocks = 2048
	bdev.DriverSpecific = &spdktypes.BdevDriverSpecific{
		Lvol: &spdktypes.BdevDriverSpecificLvol{
			LvolStoreUUID: lvsUUID,
		},
	}
	return bdev
}

// buildReplicaFromObservation is the pure derivation behind ReplicaGet/List
// and the reconciler. It must work from prefetched dumps (one bdev dump + one
// subsystem dump shared across replicas) and classify expose state correctly.
func (s *TestSuite) TestBuildReplicaFromObservation(c *C) {
	fmt.Println("Testing buildReplicaFromObservation from shared SPDK dumps")

	record := &ReplicaRecord{Name: "r-1", LvsName: "lvs", LvsUUID: "lvs-uuid", IP: "10.0.0.1", PortStart: 21000, PortEnd: 21015}

	// No bdevs at all -> Stopped identity-only replica.
	r, err := buildReplicaFromObservation(record, NvmfTransportTCP, nil, nil)
	c.Assert(err, IsNil)
	c.Check(string(r.State), Equals, types.InstanceStateStopped)
	c.Check(r.Name, Equals, "r-1")
	c.Check(r.Head, IsNil)

	// Head lvol present, no listener -> Stopped with head populated.
	bdevs := []spdktypes.BdevInfo{headBdevFixture("r-1", "lvs", "lvs-uuid")}
	r, err = buildReplicaFromObservation(record, NvmfTransportTCP, bdevs, nil)
	c.Assert(err, IsNil)
	c.Check(string(r.State), Equals, types.InstanceStateStopped)
	c.Assert(r.Head, NotNil)
	c.Check(r.IsExposed, Equals, false)

	// Head lvol present + listener in the shared subsystem dump -> Running.
	subsystems := []spdktypes.NvmfSubsystem{
		{
			Nqn: helpertypes.GetNQN("r-1"),
			ListenAddresses: []spdktypes.NvmfSubsystemListenAddress{
				{Trtype: spdktypes.NvmeTransportTypeTCP, Traddr: "10.0.0.1", Trsvcid: "21000"},
			},
		},
		// Another replica's subsystem must not leak into this derivation.
		{Nqn: helpertypes.GetNQN("r-2"), ListenAddresses: []spdktypes.NvmfSubsystemListenAddress{{Traddr: "10.0.0.1", Trsvcid: "22000"}}},
	}
	r, err = buildReplicaFromObservation(record, NvmfTransportTCP, bdevs, subsystems)
	c.Assert(err, IsNil)
	c.Check(string(r.State), Equals, types.InstanceStateRunning)
	c.Check(r.IsExposed, Equals, true)
}

// The derived replica must carry a real logger: ServiceReplicaToProtoReplica
// logs through r.log when the BackingImage name fails to parse, which is
// reachable from the ReplicaGet/List gRPC read path and used to nil-deref.
func (s *TestSuite) TestDerivedReplicaLoggerIsUsable(c *C) {
	fmt.Println("Testing derived replica logger is non-nil and safe to use")

	record := &ReplicaRecord{Name: "r-log", LvsName: "lvs", LvsUUID: "lvs-uuid"}
	r := newReplicaSkeletonFromRecord(record, NvmfTransportTCP)
	c.Assert(r.log, NotNil)

	// Force the error path that logs via r.log: a BackingImage whose name
	// does not match the bi-<name>-disk-<uuid> pattern.
	r.BackingImage = &Lvol{Name: "not-a-backing-image-name"}
	proto := ServiceReplicaToProtoReplica(r) // must not panic
	c.Assert(proto, NotNil)
	c.Check(proto.BackingImageName, Equals, "")
}
