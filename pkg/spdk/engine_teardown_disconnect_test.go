package spdk

import (
	"fmt"

	commonns "github.com/longhorn/go-common-libs/ns"

	. "gopkg.in/check.v1"
)

// withDisconnectSeams swaps the engineNewExecutor / engineDisconnectController
// hooks for the duration of fn and restores them afterwards. The io_hooks
// variables are not parallel-safe, which matches the rest of this suite.
func withDisconnectSeams(newExec func(string) (*commonns.Executor, error),
	disconnect func(string, string, string, *commonns.Executor) error, fn func()) {
	origExec, origDisconnect := engineNewExecutor, engineDisconnectController
	defer func() { engineNewExecutor, engineDisconnectController = origExec, origDisconnect }()
	engineNewExecutor, engineDisconnectController = newExec, disconnect
	fn()
}

// An empty NQN, empty IP, or zero port means there is no local controller to
// drop, so the teardown helper must be a pure no-op -- it must not even create
// an executor (which would touch the host).
func (s *TestSuite) TestDisconnectLocalTargetControllerNoopOnEmptyAddress(c *C) {
	for _, tc := range []struct {
		nqn, ip string
		port    int32
	}{
		{"", "10.0.0.1", 2000},
		{"nqn.test", "", 2000},
		{"nqn.test", "10.0.0.1", 0},
	} {
		execCalled, disconnectCalled := false, false
		withDisconnectSeams(
			func(string) (*commonns.Executor, error) { execCalled = true; return nil, nil },
			func(string, string, string, *commonns.Executor) error { disconnectCalled = true; return nil },
			func() {
				c.Assert(disconnectLocalTargetController(tc.nqn, tc.ip, tc.port), IsNil)
			},
		)
		c.Check(execCalled, Equals, false)
		c.Check(disconnectCalled, Equals, false)
	}
}

// The safety-critical property: the disconnect is addressed by the engine's
// EXACT (nqn, ip, port) -- so a freshly re-homed EngineFrontend on a different
// target address for the same (stable) volume NQN is never dropped. The port is
// rendered as its decimal string and the created executor is passed through.
func (s *TestSuite) TestDisconnectLocalTargetControllerTargetsExactAddress(c *C) {
	sentinel := &commonns.Executor{}
	var gotNQN, gotIP, gotPort string
	var gotExecutor *commonns.Executor

	withDisconnectSeams(
		func(string) (*commonns.Executor, error) { return sentinel, nil },
		func(nqn, ip, port string, ex *commonns.Executor) error {
			gotNQN, gotIP, gotPort, gotExecutor = nqn, ip, port, ex
			return nil
		},
		func() {
			c.Assert(disconnectLocalTargetController("nqn.test:vol-a", "10.0.0.7", 4421), IsNil)
		},
	)

	c.Check(gotNQN, Equals, "nqn.test:vol-a")
	c.Check(gotIP, Equals, "10.0.0.7")
	c.Check(gotPort, Equals, "4421")
	c.Check(gotExecutor, Equals, sentinel)
}

// A failure to create the executor is reported (and the disconnect is never
// attempted) -- the caller logs it best-effort and proceeds to StopExposeBdev.
func (s *TestSuite) TestDisconnectLocalTargetControllerExecutorError(c *C) {
	disconnectCalled := false
	withDisconnectSeams(
		func(string) (*commonns.Executor, error) { return nil, fmt.Errorf("no host proc") },
		func(string, string, string, *commonns.Executor) error { disconnectCalled = true; return nil },
		func() {
			err := disconnectLocalTargetController("nqn.test", "10.0.0.7", 4421)
			c.Assert(err, NotNil)
			c.Assert(err, ErrorMatches, ".*no host proc.*")
		},
	)
	c.Check(disconnectCalled, Equals, false)
}

// A disconnect failure propagates unchanged (again handled best-effort by the
// caller). This is the path that, before the fix, never ran at all -- leaving
// the controller to ghost and wedge nvmf_delete_subsystem.
func (s *TestSuite) TestDisconnectLocalTargetControllerDisconnectError(c *C) {
	withDisconnectSeams(
		func(string) (*commonns.Executor, error) { return &commonns.Executor{}, nil },
		func(string, string, string, *commonns.Executor) error { return fmt.Errorf("nvme disconnect failed") },
		func() {
			err := disconnectLocalTargetController("nqn.test", "10.0.0.7", 4421)
			c.Assert(err, NotNil)
			c.Assert(err, ErrorMatches, ".*nvme disconnect failed.*")
		},
	)
}
