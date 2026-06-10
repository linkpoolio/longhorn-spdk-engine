package spdk

import (
	"fmt"

	. "gopkg.in/check.v1"

	"github.com/longhorn/longhorn-spdk-engine/pkg/types"
)

// captureBitmapsForFaultedReplicasNoLock must only act on replicas that
// transitioned into ERR during the current validation pass: replicas that
// were already ERR (bitmap captured earlier or transition pre-dates
// tracking), replicas without a previous mode, healthy replicas, and
// replicas without a recorded bdev name are all skipped. None of the skip
// paths touch SPDK, which the nil client below enforces (a wrongly-entered
// capture would nil-deref).
func (s *TestSuite) TestCaptureBitmapsForFaultedReplicasGating(c *C) {
	fmt.Println("Testing captureBitmapsForFaultedReplicasNoLock gating")

	e := NewEngine("engine-bm", "vol-bm", types.FrontendSPDKTCPNvmf, 1024, NvmfTransportTCP, make(chan interface{}, 1), defaultTestSnapshotMaxCount, nil)
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

// snapshotReplicaModesNoLock captures the per-replica mode for transition
// detection and must skip nil entries.
func (s *TestSuite) TestSnapshotReplicaModesNoLock(c *C) {
	fmt.Println("Testing snapshotReplicaModesNoLock")

	e := NewEngine("engine-bm2", "vol-bm2", types.FrontendSPDKTCPNvmf, 1024, NvmfTransportTCP, make(chan interface{}, 1), defaultTestSnapshotMaxCount, nil)
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
