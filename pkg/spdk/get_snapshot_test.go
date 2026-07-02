package spdk

import (
	"fmt"
	"time"

	lhtypes "github.com/longhorn/longhorn-spdk-engine/pkg/types"

	. "gopkg.in/check.v1"
)

// Get must not queue behind a long-held write lock: the instance-manager
// monitor's list path calls Get on every instance, and blocking it behind a
// slow creation (kernel NVMe/dm work) starves the manager's view of the IM.
func (s *TestSuite) TestEngineGetDoesNotBlockOnHeldWriteLock(c *C) {
	fmt.Println("Testing Engine.Get does not block while the write lock is held")

	e := NewEngine("engine-a", "vol-a", lhtypes.FrontendEmpty, 1024, "tcp", make(chan interface{}, 16), 0)

	// Prime the snapshot with one clean read.
	first := e.Get()
	c.Assert(first, NotNil)

	e.Lock()
	defer e.Unlock()

	done := make(chan *struct{ name string }, 1)
	go func() {
		res := e.Get()
		done <- &struct{ name string }{res.Name}
	}()

	select {
	case got := <-done:
		c.Assert(got.name, Equals, "engine-a")
	case <-time.After(2 * time.Second):
		c.Fatal("Engine.Get blocked behind a held write lock")
	}
}

func (s *TestSuite) TestEngineFrontendGetDoesNotBlockOnHeldWriteLock(c *C) {
	fmt.Println("Testing EngineFrontend.Get does not block while the write lock is held")

	ef := NewEngineFrontend("ef-a", "engine-a", "vol-a", lhtypes.FrontendEmpty, 1024, 0, 0, make(chan interface{}, 16))

	first := ef.Get()
	c.Assert(first, NotNil)

	ef.Lock()
	defer ef.Unlock()

	done := make(chan *struct{ name string }, 1)
	go func() {
		res := ef.Get()
		done <- &struct{ name string }{res.Name}
	}()

	select {
	case got := <-done:
		c.Assert(got.name, Equals, "ef-a")
	case <-time.After(2 * time.Second):
		c.Fatal("EngineFrontend.Get blocked behind a held write lock")
	}
}
