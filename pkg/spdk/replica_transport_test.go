package spdk

import (
	"fmt"

	. "gopkg.in/check.v1"
)

// A replica must advertise its listener ports consistently so the manager can
// build a complete transport-address map (no empty/half entries): a TCP-only
// node exposes a single TCP listener at PortStart; an RDMA node exposes the
// RDMA primary at PortStart plus a TCP fallback at PortStart+1 so TCP-only
// engines can still attach. headLvolTransportAddresses must surface both as
// host:port addresses, or nil when the replica isn't exposed yet.

func (s *TestSuite) TestReplicaListenerPortsForTransport(c *C) {
	fmt.Println("Testing Replica.listenerPortsForTransport across transports")

	cases := []struct {
		name      string
		transport NvmfTransportType
		portStart int32
		wantTCP   int32
		wantRDMA  int32
	}{
		{"not exposed (PortStart=0) -> 0/0", NvmfTransportTCP, 0, 0, 0},
		{"TCP node -> single TCP listener at PortStart", NvmfTransportTCP, 21000, 21000, 0},
		{"default transport == TCP", "", 21000, 21000, 0},
		{"RDMA node -> RDMA primary + TCP fallback (PortStart+1)", NvmfTransportRDMA, 21000, 21001, 21000},
	}
	for _, tc := range cases {
		r := &Replica{IP: "10.10.5.19", PortStart: tc.portStart, ListenerTransport: tc.transport}
		tcpPort, rdmaPort := r.listenerPortsForTransport()
		c.Check(tcpPort, Equals, tc.wantTCP, Commentf("case %q: tcpPort", tc.name))
		c.Check(rdmaPort, Equals, tc.wantRDMA, Commentf("case %q: rdmaPort", tc.name))
	}
}

func (s *TestSuite) TestReplicaHeadLvolTransportAddresses(c *C) {
	fmt.Println("Testing Replica.headLvolTransportAddresses address formatting")

	// Not exposed -> nil (no half entry).
	rUnexposed := &Replica{IP: "10.10.5.19", PortStart: 0}
	c.Check(rUnexposed.headLvolTransportAddresses(), IsNil)

	// TCP node -> only TcpAddress populated.
	rTCP := &Replica{IP: "10.10.5.19", PortStart: 21000, ListenerTransport: NvmfTransportTCP}
	addrTCP := rTCP.headLvolTransportAddresses()
	c.Assert(addrTCP, NotNil)
	c.Check(addrTCP.TcpAddress, Equals, "10.10.5.19:21000")
	c.Check(addrTCP.RdmaAddress, Equals, "")

	// RDMA node -> both, RDMA at PortStart, TCP fallback at PortStart+1.
	rRDMA := &Replica{IP: "10.10.5.19", PortStart: 21000, ListenerTransport: NvmfTransportRDMA}
	addrRDMA := rRDMA.headLvolTransportAddresses()
	c.Assert(addrRDMA, NotNil)
	c.Check(addrRDMA.RdmaAddress, Equals, "10.10.5.19:21000")
	c.Check(addrRDMA.TcpAddress, Equals, "10.10.5.19:21001")
}
