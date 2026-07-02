package spdk

import (
	"fmt"

	lhtypes "github.com/longhorn/longhorn-spdk-engine/pkg/types"

	. "gopkg.in/check.v1"
)

// While a frontend connect is mid-retry the engine can be torn down and
// recreated on a new port (restored-record engines with missing raid bdevs
// routinely are). The target-moved check must flag that as soon as the new
// engine is running so the connect aborts instead of burning its full retry
// budget against the dead port, and must stay neutral in every ambiguous
// state where the normal retry/backoff is the right behavior.
func (s *TestSuite) TestEngineTargetMovedCheck(c *C) {
	fmt.Println("Testing engine target moved check")

	srv := &Server{engineMap: map[string]*Engine{}}
	check := srv.newEngineTargetMovedCheck("vol-a")

	// No engine registered for the volume: neutral.
	c.Assert(check("10.0.0.5", 20065), IsNil)

	e := NewEngine("vol-a-e-0", "vol-a", lhtypes.FrontendEmpty, 1024, "tcp", make(chan interface{}, 16), 0)
	e.NvmeTcpTarget.IP = "10.0.0.5"
	e.NvmeTcpTarget.Port = 20026
	srv.engineMap["vol-a-e-0"] = e

	// Engine present but not running (still being recreated): neutral.
	e.State = lhtypes.InstanceStatePending
	e.Get() // refresh the snapshot served by the check
	c.Assert(check("10.0.0.5", 20065), IsNil)

	// Engine running at a different port: the dial is doomed, abort.
	e.State = lhtypes.InstanceStateRunning
	e.Get()
	err := check("10.0.0.5", 20065)
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Matches, ".*target moved to 10.0.0.5:20026 while dialing 10.0.0.5:20065.*")

	// Engine running at a different IP: also moved.
	err = check("10.0.0.9", 20026)
	c.Assert(err, NotNil)

	// Engine running at exactly the dialed target: keep connecting.
	c.Assert(check("10.0.0.5", 20026), IsNil)

	// Engine running without a target listener (port 0): neutral.
	e.NvmeTcpTarget.Port = 0
	e.Get()
	c.Assert(check("10.0.0.5", 20065), IsNil)

	// A different volume's engine must never influence the check.
	otherCheck := srv.newEngineTargetMovedCheck("vol-b")
	e.NvmeTcpTarget.Port = 20026
	e.Get()
	c.Assert(otherCheck("10.0.0.5", 20065), IsNil)
}

// The per-connect abort callback composes eviction with the server's
// target-moved check: a recovering frontend evicted by a concurrent Create
// aborts regardless of the target, and a frontend without a server-installed
// check never aborts.
func (s *TestSuite) TestConnectAbortCheck(c *C) {
	fmt.Println("Testing engine frontend connect abort check")

	srv := &Server{engineMap: map[string]*Engine{}}

	e := NewEngine("vol-a-e-0", "vol-a", lhtypes.FrontendEmpty, 1024, "tcp", make(chan interface{}, 16), 0)
	e.State = lhtypes.InstanceStateRunning
	e.NvmeTcpTarget.IP = "10.0.0.5"
	e.NvmeTcpTarget.Port = 20026
	e.Get()
	srv.engineMap["vol-a-e-0"] = e

	ef := NewEngineFrontend("vol-a-ef-0", "vol-a-e-0", "vol-a", lhtypes.FrontendSPDKTCPBlockdev,
		1024, 0, 0, make(chan interface{}, 16))
	ef.targetMovedCheck = srv.newEngineTargetMovedCheck("vol-a")

	// Recovery in progress (Pending), target still current: keep going.
	ef.State = lhtypes.InstanceStatePending
	c.Assert(ef.newConnectAbortCheck("10.0.0.5", 20026, true)(), IsNil)

	// Recovery in progress, target moved: abort with the moved error.
	err := ef.newConnectAbortCheck("10.0.0.5", 20065, true)()
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Matches, ".*target moved.*")

	// Evicted by a concurrent Create (state left Pending): abort immediately,
	// even when the target looks current.
	ef.State = lhtypes.InstanceStateTerminating
	c.Assert(ef.newConnectAbortCheck("10.0.0.5", 20026, true)(), Equals, ErrRecoveryCancelled)

	// The Create path does not consult recovery cancellation.
	c.Assert(ef.newConnectAbortCheck("10.0.0.5", 20026, false)(), IsNil)

	// No server-installed check (e.g. constructed outside the server): never abort.
	bare := NewEngineFrontend("vol-c-ef-0", "vol-c-e-0", "vol-c", lhtypes.FrontendSPDKTCPBlockdev,
		1024, 0, 0, make(chan interface{}, 16))
	bare.State = lhtypes.InstanceStatePending
	c.Assert(bare.newConnectAbortCheck("10.0.0.1", 1, false)(), IsNil)
}
