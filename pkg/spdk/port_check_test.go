package spdk

import (
	"fmt"

	commonbitmap "github.com/longhorn/go-common-libs/bitmap"

	. "gopkg.in/check.v1"
)

// On host-network instance managers the kernel can squat any Longhorn port
// with an ephemeral source port (the ma3-worker-11 EADDRINUSE storm of
// 2026-07-02). Allocation must skip squatted ports, permanently taint the
// ranges that contained them, and only ever hand out ranges it has proven
// bindable.
func (s *TestSuite) TestAllocateUsablePortRange(c *C) {
	fmt.Println("Testing usable port range allocation")

	origBind := testPortBindFn
	defer func() { testPortBindFn = origBind }()

	squatted := map[int32]bool{20001: true, 20003: true}
	var probed []int32
	testPortBindFn = func(ip string, port int32) error {
		probed = append(probed, port)
		c.Assert(ip, Equals, "10.0.0.5")
		if squatted[port] {
			return fmt.Errorf("bind: address already in use")
		}
		return nil
	}

	allocator, err := commonbitmap.NewBitmap(20001, 20010)
	c.Assert(err, IsNil)

	// 20001 squatted -> tainted; 20002 clean -> handed out.
	start, end, err := allocateUsablePortRange(allocator, "10.0.0.5", 1, "test")
	c.Assert(err, IsNil)
	c.Assert(start, Equals, int32(20002))
	c.Assert(end, Equals, int32(20002))

	// Tainted 20001 must never be offered again: next allocation skips to
	// 20003 (squatted -> tainted) then 20004.
	start, _, err = allocateUsablePortRange(allocator, "10.0.0.5", 1, "test")
	c.Assert(err, IsNil)
	c.Assert(start, Equals, int32(20004))

	// Multi-port range: all ports in the range are probed.
	probed = probed[:0]
	start, end, err = allocateUsablePortRange(allocator, "10.0.0.5", 3, "test")
	c.Assert(err, IsNil)
	c.Assert(start, Equals, int32(20005))
	c.Assert(end, Equals, int32(20007))
	c.Assert(probed, DeepEquals, []int32{20005, 20006, 20007})

	// Exhaustion: squat everything left; the allocator error surfaces.
	for p := int32(20008); p <= 20010; p++ {
		squatted[p] = true
	}
	_, _, err = allocateUsablePortRange(allocator, "10.0.0.5", 1, "test")
	c.Assert(err, NotNil)
}

// The SPDK expose backstop keys on the generic "Invalid parameters" RPC
// response — the only signal SPDK gives for a failed listener bind.
func (s *TestSuite) TestIsListenerBindConflict(c *C) {
	fmt.Println("Testing listener bind conflict classification")

	c.Assert(isListenerBindConflict(fmt.Errorf(`error sending message, method nvmf_subsystem_add_listener: {"code": -32602,"message": "Invalid parameters"}`)), Equals, true)
	c.Assert(isListenerBindConflict(fmt.Errorf("connection refused")), Equals, false)
	c.Assert(isListenerBindConflict(nil), Equals, false)
}
