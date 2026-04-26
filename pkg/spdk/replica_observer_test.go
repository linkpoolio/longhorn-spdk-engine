package spdk

import (
	. "gopkg.in/check.v1"

	spdktypes "github.com/longhorn/go-spdk-helper/pkg/spdk/types"
)

func (s *TestSuite) TestLvolBaseName(c *C) {
	c.Check(lvolBaseName(nil), Equals, "")
	c.Check(lvolBaseName(&spdktypes.BdevInfo{}), Equals, "")
	c.Check(lvolBaseName(&spdktypes.BdevInfo{
		BdevInfoBasic: spdktypes.BdevInfoBasic{Aliases: []string{"longhorn-data/pvc-58a5db03-r-30cfff06"}},
	}), Equals, "pvc-58a5db03-r-30cfff06")
	c.Check(lvolBaseName(&spdktypes.BdevInfo{
		BdevInfoBasic: spdktypes.BdevInfoBasic{Aliases: []string{"no-slash-here"}},
	}), Equals, "no-slash-here")
}

func (s *TestSuite) TestIsReplicaBdev(c *C) {
	const replicaName = "pvc-58a5db03-r-30cfff06"
	const lvsUUID = "lvs-uuid-1"

	headBdev := &spdktypes.BdevInfo{
		BdevInfoBasic: spdktypes.BdevInfoBasic{Aliases: []string{"longhorn-data/" + replicaName}},
		DriverSpecific: &spdktypes.BdevDriverSpecific{
			Lvol: &spdktypes.BdevDriverSpecificLvol{LvolStoreUUID: lvsUUID},
		},
	}
	c.Check(isReplicaBdev(headBdev, replicaName, lvsUUID), Equals, true)

	snapBdev := &spdktypes.BdevInfo{
		BdevInfoBasic: spdktypes.BdevInfoBasic{Aliases: []string{"longhorn-data/" + replicaName + "-snap-abc123"}},
		DriverSpecific: &spdktypes.BdevDriverSpecific{
			Lvol: &spdktypes.BdevDriverSpecificLvol{LvolStoreUUID: lvsUUID},
		},
	}
	c.Check(isReplicaBdev(snapBdev, replicaName, lvsUUID), Equals, true)

	// Different lvstore: should reject even with matching name.
	wrongLvs := &spdktypes.BdevInfo{
		BdevInfoBasic: spdktypes.BdevInfoBasic{Aliases: []string{"longhorn-data/" + replicaName}},
		DriverSpecific: &spdktypes.BdevDriverSpecific{
			Lvol: &spdktypes.BdevDriverSpecificLvol{LvolStoreUUID: "different-lvs"},
		},
	}
	c.Check(isReplicaBdev(wrongLvs, replicaName, lvsUUID), Equals, false)

	// Different replica name on same lvstore: should reject.
	otherReplica := &spdktypes.BdevInfo{
		BdevInfoBasic: spdktypes.BdevInfoBasic{Aliases: []string{"longhorn-data/pvc-OTHER-r-99"}},
		DriverSpecific: &spdktypes.BdevDriverSpecific{
			Lvol: &spdktypes.BdevDriverSpecificLvol{LvolStoreUUID: lvsUUID},
		},
	}
	c.Check(isReplicaBdev(otherReplica, replicaName, lvsUUID), Equals, false)

	// nil / no driver-specific / no aliases: false (defensive).
	c.Check(isReplicaBdev(nil, replicaName, lvsUUID), Equals, false)
	c.Check(isReplicaBdev(&spdktypes.BdevInfo{}, replicaName, lvsUUID), Equals, false)
	c.Check(isReplicaBdev(&spdktypes.BdevInfo{
		BdevInfoBasic: spdktypes.BdevInfoBasic{Aliases: []string{}},
		DriverSpecific: &spdktypes.BdevDriverSpecific{
			Lvol: &spdktypes.BdevDriverSpecificLvol{LvolStoreUUID: lvsUUID},
		},
	}, replicaName, lvsUUID), Equals, false)
}
