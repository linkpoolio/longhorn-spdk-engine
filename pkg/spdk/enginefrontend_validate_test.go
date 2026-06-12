package spdk

import (
	"fmt"

	lhtypes "github.com/longhorn/longhorn-spdk-engine/pkg/types"

	. "gopkg.in/check.v1"
)

// TestEngineFrontendValidationFailureTolerance verifies that transient
// validation failures do not immediately fault the engine frontend: only
// maxConsecutiveValidationFailures back-to-back failures flip it to Error
// (2026-06-12 cascade: kernel NVMe device churn from one volume detaching
// faulted every other volume on the node on the first failed tick).
func (s *TestSuite) TestEngineFrontendValidationFailureTolerance(c *C) {
	fmt.Println("Testing EngineFrontend validation failure tolerance")

	ef := NewEngineFrontend("ef-a", "engine-a", "vol-a", "bogus-frontend", 1024, 0, 0, make(chan interface{}, 4096))
	ef.State = lhtypes.InstanceStateRunning

	for i := 1; i < maxConsecutiveValidationFailures; i++ {
		err := ef.ValidateAndUpdate(nil)
		c.Assert(err, IsNil, Commentf("failure %d should be tolerated", i))
		c.Assert(string(ef.State), Equals, string(lhtypes.InstanceStateRunning))
		c.Assert(ef.ErrorMsg, Equals, "")
		c.Assert(ef.consecutiveValidationFailures, Equals, i)
	}

	err := ef.ValidateAndUpdate(nil)
	c.Assert(err, NotNil)
	c.Assert(string(ef.State), Equals, string(lhtypes.InstanceStateError))
	c.Assert(ef.ErrorMsg, Not(Equals), "")
}

// TestEngineFrontendValidationFailureCounterResets verifies a successful
// validation tick resets the consecutive-failure counter, so sporadic
// non-consecutive glitches never accumulate into a fault.
func (s *TestSuite) TestEngineFrontendValidationFailureCounterResets(c *C) {
	fmt.Println("Testing EngineFrontend validation failure counter reset")

	ef := NewEngineFrontend("ef-a", "engine-a", "vol-a", lhtypes.FrontendEmpty, 1024, 0, 0, make(chan interface{}, 4096))
	ef.State = lhtypes.InstanceStateRunning
	ef.consecutiveValidationFailures = maxConsecutiveValidationFailures - 1

	err := ef.ValidateAndUpdate(nil)
	c.Assert(err, IsNil)
	c.Assert(ef.consecutiveValidationFailures, Equals, 0)
	c.Assert(string(ef.State), Equals, string(lhtypes.InstanceStateRunning))
}
