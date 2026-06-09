package spdk

import (
	"fmt"

	. "gopkg.in/check.v1"

	"github.com/longhorn/types/pkg/generated/spdkrpc"
)

// The engine picks the address+transport to dial each replica. When the
// replica advertises typed transport addresses (storage IM reports its ports),
// the map is the source of truth. When it does not (an older, transport-unaware
// storage IM), the engine falls back to the addressing convention: the legacy
// address is the primary listener (RDMA on an RDMA storage node), with a TCP
// fallback at primary+1. An RDMA engine dials the legacy address over RDMA; a
// TCP engine dials primary+1 over TCP. This is the pre-rebase behaviour and is
// what keeps a rebased engine backward-compatible with old storage IMs.

func (s *TestSuite) TestEnginePickReplicaAddress(c *C) {
	fmt.Println("Testing Engine.pickReplicaAddress: CRD map when present, +1 TCP fallback when absent")

	const (
		legacy   = "10.10.3.19:28923" // primary (RDMA on RDMA storage)
		tcpFB    = "10.10.3.19:28924" // primary+1 (TCP fallback)
		tcpAddr  = "10.10.3.19:30000" // an explicitly advertised TCP address
		rdmaAddr = "10.10.3.19:28923"
	)
	both := &spdkrpc.ReplicaTransportAddresses{TcpAddress: tcpAddr, RdmaAddress: rdmaAddr}
	tcpOnly := &spdkrpc.ReplicaTransportAddresses{TcpAddress: tcpAddr}
	rdmaOnly := &spdkrpc.ReplicaTransportAddresses{RdmaAddress: rdmaAddr}
	empty := &spdkrpc.ReplicaTransportAddresses{}

	cases := []struct {
		name      string
		engineTr  NvmfTransportType
		present   bool
		entry     *spdkrpc.ReplicaTransportAddresses
		wantAddr  string
		wantTrans NvmfTransportType
	}{
		// No map entry (old/transport-unaware storage): convention fallback.
		{"no map, TCP engine -> primary+1/TCP", NvmfTransportTCP, false, nil, tcpFB, NvmfTransportTCP},
		{"no map, RDMA engine -> primary/RDMA", NvmfTransportRDMA, false, nil, legacy, NvmfTransportRDMA},
		{"nil entry, TCP engine -> primary+1/TCP", NvmfTransportTCP, true, nil, tcpFB, NvmfTransportTCP},
		// Advertised both: engine transport decides.
		{"both, RDMA engine -> rdma/RDMA", NvmfTransportRDMA, true, both, rdmaAddr, NvmfTransportRDMA},
		{"both, TCP engine -> tcp/TCP", NvmfTransportTCP, true, both, tcpAddr, NvmfTransportTCP},
		// TCP-only advertised: TCP for either engine.
		{"tcp-only, RDMA engine -> tcp/TCP", NvmfTransportRDMA, true, tcpOnly, tcpAddr, NvmfTransportTCP},
		// RDMA-only advertised: RDMA engine uses it; TCP engine falls back to +1.
		{"rdma-only, RDMA engine -> rdma/RDMA", NvmfTransportRDMA, true, rdmaOnly, rdmaAddr, NvmfTransportRDMA},
		{"rdma-only, TCP engine -> primary+1/TCP", NvmfTransportTCP, true, rdmaOnly, tcpFB, NvmfTransportTCP},
		// Entry present but empty -> convention fallback.
		{"empty entry, TCP engine -> primary+1/TCP", NvmfTransportTCP, true, empty, tcpFB, NvmfTransportTCP},
		{"empty entry, RDMA engine -> primary/RDMA", NvmfTransportRDMA, true, empty, legacy, NvmfTransportRDMA},
	}

	for _, tc := range cases {
		e := &Engine{ReplicaTransport: tc.engineTr}
		m := map[string]*spdkrpc.ReplicaTransportAddresses{}
		if tc.present {
			m["r-1"] = tc.entry
		}
		addr, trans := e.pickReplicaAddress("r-1", legacy, m)
		c.Check(addr, Equals, tc.wantAddr, Commentf("case %q: address", tc.name))
		c.Check(trans, Equals, tc.wantTrans, Commentf("case %q: transport", tc.name))
	}
}

func (s *TestSuite) TestEngineLegacyTransportFallback(c *C) {
	fmt.Println("Testing Engine.legacyTransportFallback (backward-compat convention)")

	// TCP engine -> primary+1 over TCP.
	tcpEng := &Engine{ReplicaTransport: NvmfTransportTCP}
	addr, trans := tcpEng.legacyTransportFallback("10.10.5.19:21099")
	c.Check(addr, Equals, "10.10.5.19:21100")
	c.Check(trans, Equals, NvmfTransportTCP)

	// RDMA engine -> primary as-is over RDMA.
	rdmaEng := &Engine{ReplicaTransport: NvmfTransportRDMA}
	addr, trans = rdmaEng.legacyTransportFallback("10.10.5.19:21099")
	c.Check(addr, Equals, "10.10.5.19:21099")
	c.Check(trans, Equals, NvmfTransportRDMA)

	// Unparseable address -> as-is over TCP (last resort, no crash).
	addr, trans = tcpEng.legacyTransportFallback("garbage-no-port")
	c.Check(addr, Equals, "garbage-no-port")
	c.Check(trans, Equals, NvmfTransportTCP)
}
