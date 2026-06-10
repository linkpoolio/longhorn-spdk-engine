package spdk

import (
	"context"
	"fmt"

	. "gopkg.in/check.v1"
)

// The superior port allocator must be seeded with the port ranges persisted
// by replicas and engine targets before any create can run; otherwise a fresh
// create after an spdk_tgt restart can collide with a restored instance's
// ports.
func (s *TestSuite) TestNewPortAllocatorWithReservationsRefusesReservedRange(c *C) {
	fmt.Println("Testing newPortAllocatorWithReservations refuses to hand out reserved ranges")

	const (
		portStart int32 = 100
		portEnd   int32 = 119
	)
	reservations := []reservedPortRange{
		{name: "replica r-1", start: 104, end: 108},
		{name: "engine e-1", start: 110, end: 110},
	}

	b, err := newPortAllocatorWithReservations(portStart, portEnd, reservations)
	c.Assert(err, IsNil)

	reserved := map[int32]bool{104: true, 105: true, 106: true, 107: true, 108: true, 110: true}

	// Drain the allocator one port at a time; none of the handed-out ports
	// may fall inside a reserved range, and the total must equal the range
	// size minus the reservations.
	allocated := map[int32]bool{}
	for {
		p, _, allocErr := b.AllocateRange(1)
		if allocErr != nil {
			break
		}
		c.Assert(allocated[p], Equals, false, Commentf("port %d handed out twice", p))
		allocated[p] = true
		c.Assert(reserved[p], Equals, false, Commentf("reserved port %d handed out", p))
	}
	c.Check(len(allocated), Equals, int(portEnd-portStart+1)-len(reserved))
}

func (s *TestSuite) TestNewPortAllocatorWithReservationsSkipsOutOfRange(c *C) {
	fmt.Println("Testing newPortAllocatorWithReservations skips out-of-range reservations")

	// A reservation outside the allocator window must be skipped (warn), not
	// fail the allocator; the full window stays allocatable.
	b, err := newPortAllocatorWithReservations(100, 103, []reservedPortRange{
		{name: "replica stale", start: 200, end: 210},
		{name: "replica inverted", start: 103, end: 101},
	})
	c.Assert(err, IsNil)
	start, end, err := b.AllocateRange(4)
	c.Assert(err, IsNil)
	c.Check(start, Equals, int32(100))
	c.Check(end, Equals, int32(103))
}

// collectReservedPortRanges must consider both replica records (their full
// PortStart..PortEnd range) and engine records (the target's single listener
// port) since both allocate from the same superior allocator.
func (s *TestSuite) TestCollectReservedPortRanges(c *C) {
	fmt.Println("Testing collectReservedPortRanges covers replica and engine records")

	replicaRecords := map[string]*ReplicaRecord{
		"r-1": {Name: "r-1", PortStart: 20001, PortEnd: 20015},
		"r-0": {Name: "r-0", PortStart: 0, PortEnd: 0}, // never exposed: no reservation
	}
	engineRecords := map[string]*EngineRecord{
		"e-1": {Name: "e-1", NvmeTcpTarget: &NvmeTcpTarget{Port: 20020}},
		"e-0": {Name: "e-0", NvmeTcpTarget: &NvmeTcpTarget{Port: 0}}, // no listener
		"e-n": {Name: "e-n"},                                         // no target at all
	}

	ranges := collectReservedPortRanges(replicaRecords, engineRecords)
	c.Assert(len(ranges), Equals, 2)

	got := map[string][2]int32{}
	for _, r := range ranges {
		got[r.name] = [2]int32{r.start, r.end}
	}
	c.Check(got["replica r-1"], Equals, [2]int32{20001, 20015})
	c.Check(got["engine e-1"], Equals, [2]int32{20020, 20020})
}

// On dual-listener (RDMA) replicas the TCP fallback listener lives at
// PortStart+1, so the local rebuild/clone allocator must start at PortStart+2
// on both the fresh-create and the restore-from-record paths; on TCP nodes a
// single listener occupies only PortStart.
func (s *TestSuite) TestReplicaRebuildPortAllocatorStart(c *C) {
	fmt.Println("Testing Replica.rebuildPortAllocatorStart excludes listener ports")

	cases := []struct {
		name      string
		transport NvmfTransportType
		want      int32
	}{
		{"TCP node -> PortStart+1", NvmfTransportTCP, 21001},
		{"default transport (TCP) -> PortStart+1", "", 21001},
		{"RDMA node -> PortStart+2 (skips TCP fallback listener)", NvmfTransportRDMA, 21002},
	}
	for _, tc := range cases {
		r := &Replica{PortStart: 21000, PortEnd: 21015, ListenerTransport: tc.transport}
		c.Check(r.rebuildPortAllocatorStart(), Equals, tc.want, Commentf("case %q", tc.name))
	}

	// Fresh-create and restore must agree: a restored RDMA replica's first
	// rebuild port allocation must not be the TCP fallback listener port.
	r := NewReplica(context.Background(), "r-rdma", "lvs", "lvs-uuid", 1024, false, NvmfTransportRDMA, make(chan interface{}, 1), nil)
	err := r.restoreFromRecord(&ReplicaRecord{Name: "r-rdma", IP: "10.0.0.1", PortStart: 21000, PortEnd: 21015})
	c.Assert(err, IsNil)
	p, _, err := r.portAllocator.AllocateRange(1)
	c.Assert(err, IsNil)
	c.Check(p, Equals, int32(21002))
}
