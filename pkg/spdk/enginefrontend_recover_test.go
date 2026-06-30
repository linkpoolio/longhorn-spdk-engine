package spdk

import (
	"fmt"
	"sync/atomic"

	. "gopkg.in/check.v1"

	lhtypes "github.com/longhorn/longhorn-spdk-engine/pkg/types"
)

func (s *TestSuite) TestRecoverFromHostDisconnectsStaleNVMeControllers(c *C) {
	fmt.Println("Testing RecoverFromHost disconnects stale kernel NVMe controllers before creating new initiator")

	// Track whether the stale disconnect was called and with what NQN
	var disconnectCalled int32
	var disconnectNQN string

	origDisconnect := disconnectStaleNVMeControllers
	defer func() {
		disconnectStaleNVMeControllers = origDisconnect
	}()
	disconnectStaleNVMeControllers = func(nqn string) error {
		atomic.StoreInt32(&disconnectCalled, 1)
		disconnectNQN = nqn
		return nil
	}

	// Create an EF with a blockdev frontend
	ef := NewEngineFrontend("ef-test", "engine-test", "vol-test",
		lhtypes.FrontendSPDKTCPBlockdev, 1024*1024*1024, 0, 0,
		make(chan interface{}, 4096))
	ef.State = lhtypes.InstanceStatePending

	// RecoverFromHost will fail because newNvmeTcpInitiator tries to create
	// a real initiator, but the important thing is that disconnectStaleNVMeControllers
	// is called BEFORE the initiator creation attempt.
	_ = ef.RecoverFromHost(nil)

	// Verify the stale disconnect was called with the correct NQN
	c.Assert(atomic.LoadInt32(&disconnectCalled), Equals, int32(1))
	expectedNQN := "nqn.2023-01.io.longhorn.spdk:volume-vol-test"
	c.Assert(disconnectNQN, Equals, expectedNQN)
}

func (s *TestSuite) TestRecoverFromHostContinuesOnDisconnectError(c *C) {
	fmt.Println("Testing RecoverFromHost continues when stale NVMe disconnect fails")

	origDisconnect := disconnectStaleNVMeControllers
	defer func() {
		disconnectStaleNVMeControllers = origDisconnect
	}()
	disconnectStaleNVMeControllers = func(nqn string) error {
		return fmt.Errorf("simulated disconnect failure")
	}

	// Create an EF with a blockdev frontend
	ef := NewEngineFrontend("ef-test", "engine-test", "vol-test",
		lhtypes.FrontendSPDKTCPBlockdev, 1024*1024*1024, 0, 0,
		make(chan interface{}, 4096))
	ef.State = lhtypes.InstanceStatePending

	// RecoverFromHost should not panic or hang — it should continue
	// past the disconnect failure and attempt initiator creation
	// (which will also fail since there's no real host, but that's fine)
	err := ef.RecoverFromHost(nil)

	// The error should be from the initiator creation, not from the disconnect
	c.Assert(err, NotNil)
	// The EF should not be in Pending state anymore — recovery attempted and failed
	c.Assert(string(ef.State), Not(Equals), string(lhtypes.InstanceStatePending))
}

func (s *TestSuite) TestRecoverFromHostSkipsDisconnectForEmptyFrontend(c *C) {
	fmt.Println("Testing RecoverFromHost skips stale NVMe disconnect for empty frontend")

	var disconnectCalled int32

	origDisconnect := disconnectStaleNVMeControllers
	defer func() {
		disconnectStaleNVMeControllers = origDisconnect
	}()
	disconnectStaleNVMeControllers = func(nqn string) error {
		atomic.StoreInt32(&disconnectCalled, 1)
		return nil
	}

	// Create an EF with an empty frontend (no initiator)
	ef := NewEngineFrontend("ef-test", "engine-test", "vol-test",
		lhtypes.FrontendEmpty, 1024*1024*1024, 0, 0,
		make(chan interface{}, 4096))
	ef.State = lhtypes.InstanceStatePending

	err := ef.RecoverFromHost(nil)
	c.Assert(err, IsNil)

	// The disconnect should NOT be called for empty frontend
	c.Assert(atomic.LoadInt32(&disconnectCalled), Equals, int32(0))
}
