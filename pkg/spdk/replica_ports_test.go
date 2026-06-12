package spdk

import (
	"fmt"
	"os"

	"github.com/sirupsen/logrus"

	commonbitmap "github.com/longhorn/go-common-libs/bitmap"

	"github.com/longhorn/longhorn-spdk-engine/pkg/log"

	. "gopkg.in/check.v1"
)

// The replica port geometry must match the pre-rebase line: the superior
// allocation is portCount+1 (the extra slot is the TCP fallback listener at
// PortStart+1 on dual-listener nodes), rebuild/clone exposes allocate listener
// pairs from the local allocator, and a replica that already owns a range
// (restored from a persisted record) must keep it instead of leaking the
// reservation by reallocating.

func newTestPortReplica(transport NvmfTransportType) *Replica {
	return &Replica{
		Name:              "test-replica-r-0",
		ListenerTransport: transport,
		log:               log.NewSafeLogger(logrus.StandardLogger().WithField("test", true)),
	}
}

func (s *TestSuite) TestPrepareIPAndPortsAllocatesFallbackSlot(c *C) {
	fmt.Println("Testing Replica.prepareIPAndPorts allocates portCount+1 with listener reservation")

	c.Assert(os.Setenv("POD_IP", "10.10.5.19"), IsNil)
	defer func() {
		c.Assert(os.Unsetenv("POD_IP"), IsNil)
	}()

	superior, err := commonbitmap.NewBitmap(20000, 20100)
	c.Assert(err, IsNil)

	r := newTestPortReplica(NvmfTransportRDMA)
	c.Assert(r.prepareIPAndPorts(5, superior), IsNil)

	// portCount+1 = 6 ports inclusive: [PortStart, PortStart+5].
	c.Check(r.PortEnd-r.PortStart, Equals, int32(5))

	// The local rebuild/clone allocator must start past the listener pair and
	// be able to hand out two pairs (rebuild dst + src roles on one replica).
	first, _, err := r.portAllocator.AllocateRange(2)
	c.Assert(err, IsNil)
	c.Check(first, Equals, r.PortStart+2)
	second, _, err := r.portAllocator.AllocateRange(2)
	c.Assert(err, IsNil)
	c.Check(second, Equals, r.PortStart+4)
}

func (s *TestSuite) TestPrepareIPAndPortsReusesReservedRange(c *C) {
	fmt.Println("Testing Replica.prepareIPAndPorts reuses an already-owned port range")

	c.Assert(os.Setenv("POD_IP", "10.10.5.19"), IsNil)
	defer func() {
		c.Assert(os.Unsetenv("POD_IP"), IsNil)
	}()

	superior, err := commonbitmap.NewBitmap(20000, 20100)
	c.Assert(err, IsNil)

	r := newTestPortReplica(NvmfTransportRDMA)
	c.Assert(r.prepareIPAndPorts(5, superior), IsNil)
	portStart, portEnd := r.PortStart, r.PortEnd

	// Restart-in-place: a second call must keep the range and must not
	// consume anything further from the superior allocator.
	c.Assert(r.prepareIPAndPorts(5, superior), IsNil)
	c.Check(r.PortStart, Equals, portStart)
	c.Check(r.PortEnd, Equals, portEnd)

	other := newTestPortReplica(NvmfTransportRDMA)
	c.Assert(other.prepareIPAndPorts(5, superior), IsNil)
	c.Check(other.PortStart, Equals, portEnd+1)
}
