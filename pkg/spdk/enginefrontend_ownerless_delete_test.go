package spdk

import (
	"fmt"

	lhtypes "github.com/longhorn/longhorn-spdk-engine/pkg/types"

	. "gopkg.in/check.v1"
)

// A frontend deleted without a recovered initiator (volume departed the node,
// or recovery failed) is the last owner of the volume's kernel sessions: the
// record is removed by the delete, erasing the final pointer to them. The
// delete must therefore disconnect the subsystem's dead controllers — the
// path that orphaned 38 sessions on ma3-worker-11 style nodes.
func (s *TestSuite) TestOwnerlessDeleteDisconnectsDeadControllers(c *C) {
	fmt.Println("Testing ownerless delete disconnects dead controllers")

	ef := NewEngineFrontend("vol-a-ef-0", "vol-a-e-0", "vol-a", lhtypes.FrontendSPDKTCPBlockdev,
		1024, 0, 0, make(chan interface{}, 16))
	ef.State = lhtypes.InstanceStateStopped

	var gotNQN string
	calls := 0
	ef.disconnectDeadSubsystemControllersFn = func(nqn string) int {
		gotNQN = nqn
		calls++
		return 2
	}

	c.Assert(ef.Delete(nil), IsNil)
	c.Assert(calls, Equals, 1)
	expectedNQN, _ := ef.getVolumeTargetIdentity()
	c.Assert(gotNQN, Equals, expectedNQN)

	// With an initiator present the existing Stop path owns cleanup — the
	// ownerless disconnect must NOT run (Stop handles dm + sessions itself).
	// Covered implicitly: the hook is only reachable from the nil-initiator
	// branch, and initiator-present deletes are exercised by existing tests.
}
