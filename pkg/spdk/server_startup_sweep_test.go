package spdk

import (
	"fmt"

	. "gopkg.in/check.v1"

	spdktypes "github.com/longhorn/go-spdk-helper/pkg/spdk/types"
)

func (s *TestSuite) TestIsRebuildControllerName(c *C) {
	c.Assert(isRebuildControllerName("pvc-abc-r-123-snap-rebuild-456"), Equals, true)
	c.Assert(isRebuildControllerName("pvc-abc-r-123-snap-rebuild-456n1"), Equals, true)
	c.Assert(isRebuildControllerName("pvc-abc-r-123"), Equals, false)
	c.Assert(isRebuildControllerName("pvc-abc-r-123-rebuilding"), Equals, false)
	c.Assert(isRebuildControllerName(""), Equals, false)
}

func (s *TestSuite) TestGetReplicaNameFromRebuildController(c *C) {
	c.Assert(getReplicaNameFromRebuildController("pvc-abc-r-123-snap-rebuild-456"), Equals, "pvc-abc-r-123")
	c.Assert(getReplicaNameFromRebuildController("pvc-abc-r-123-snap-rebuild-456n1"), Equals, "pvc-abc-r-123")
	c.Assert(getReplicaNameFromRebuildController("pvc-abc-r-123"), Equals, "")
	c.Assert(getReplicaNameFromRebuildController(""), Equals, "")
}

func (s *TestSuite) TestHasChildren(c *C) {
	bdevLvolMap := map[string]spdktypes.BdevLvol{
		"replica-1": {},
		"replica-1-snap-abc": {
			DriverSpecific: spdktypes.BdevLvolDriverSpecific{
				Lvol: spdktypes.BdevLvolDriverSpecificLvol{
					Clones: []string{"replica-1"},
				},
			},
		},
		"replica-2": {},
	}

	// replica-1 has a snapshot that lists it as a clone
	c.Assert(hasChildren(bdevLvolMap, "replica-1"), Equals, true)
	// replica-2 has no children
	c.Assert(hasChildren(bdevLvolMap, "replica-2"), Equals, false)
}

func (s *TestSuite) TestLvolType(c *C) {
	c.Assert(lvolType("pvc-abc-r-123-rebuilding"), Equals, "rebuilding")
	c.Assert(lvolType("pvc-abc-r-123-cloning"), Equals, "cloning")
	c.Assert(lvolType("pvc-abc-r-123"), Equals, "replica")
}

func (s *TestSuite) TestStartupSweepDoneIsOneShot(c *C) {
	fmt.Println("Testing startupSweepDone atomic.Bool is one-shot")

	srv := &Server{}
	c.Assert(srv.startupSweepDone.Load(), Equals, false)

	// First call returns false (not yet done) and sets to true
	c.Assert(srv.startupSweepDone.Swap(true), Equals, false)
	c.Assert(srv.startupSweepDone.Load(), Equals, true)

	// Second call returns true (already done) — sweep should not run again
	c.Assert(srv.startupSweepDone.Swap(true), Equals, true)
	c.Assert(srv.startupSweepDone.Load(), Equals, true)
}