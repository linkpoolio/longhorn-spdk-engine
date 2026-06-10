package spdk

import (
	"fmt"

	. "gopkg.in/check.v1"

	spdktypes "github.com/longhorn/go-spdk-helper/pkg/spdk/types"
	"github.com/longhorn/types/pkg/generated/spdkrpc"

	lhtypes "github.com/longhorn/longhorn-spdk-engine/pkg/types"
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
		name       string
		engineTr   NvmfTransportType
		present    bool
		entry      *spdkrpc.ReplicaTransportAddresses
		wantAddr   string
		wantTrans  NvmfTransportType
		wantLegacy bool
	}{
		// No map entry (old/transport-unaware storage): convention fallback.
		{"no map, TCP engine -> primary+1/TCP", NvmfTransportTCP, false, nil, tcpFB, NvmfTransportTCP, true},
		{"no map, RDMA engine -> primary/RDMA", NvmfTransportRDMA, false, nil, legacy, NvmfTransportRDMA, true},
		{"nil entry, TCP engine -> primary+1/TCP", NvmfTransportTCP, true, nil, tcpFB, NvmfTransportTCP, true},
		// Advertised both: engine transport decides.
		{"both, RDMA engine -> rdma/RDMA", NvmfTransportRDMA, true, both, rdmaAddr, NvmfTransportRDMA, false},
		{"both, TCP engine -> tcp/TCP", NvmfTransportTCP, true, both, tcpAddr, NvmfTransportTCP, false},
		// TCP-only advertised: TCP for either engine.
		{"tcp-only, RDMA engine -> tcp/TCP", NvmfTransportRDMA, true, tcpOnly, tcpAddr, NvmfTransportTCP, false},
		// RDMA-only advertised: RDMA engine uses it; TCP engine falls back to +1.
		{"rdma-only, RDMA engine -> rdma/RDMA", NvmfTransportRDMA, true, rdmaOnly, rdmaAddr, NvmfTransportRDMA, false},
		{"rdma-only, TCP engine -> primary+1/TCP", NvmfTransportTCP, true, rdmaOnly, tcpFB, NvmfTransportTCP, true},
		// Entry present but empty -> convention fallback.
		{"empty entry, TCP engine -> primary+1/TCP", NvmfTransportTCP, true, empty, tcpFB, NvmfTransportTCP, true},
		{"empty entry, RDMA engine -> primary/RDMA", NvmfTransportRDMA, true, empty, legacy, NvmfTransportRDMA, true},
	}

	for _, tc := range cases {
		e := &Engine{ReplicaTransport: tc.engineTr}
		m := map[string]*spdkrpc.ReplicaTransportAddresses{}
		if tc.present {
			m["r-1"] = tc.entry
		}
		addr, trans, legacyConvention := e.pickReplicaAddress("r-1", legacy, m)
		c.Check(addr, Equals, tc.wantAddr, Commentf("case %q: address", tc.name))
		c.Check(trans, Equals, tc.wantTrans, Commentf("case %q: transport", tc.name))
		c.Check(legacyConvention, Equals, tc.wantLegacy, Commentf("case %q: legacyConvention", tc.name))
	}
}

// The runtime +1 TCP fallback after failed attach retries must fire only for
// legacy-convention RDMA dials: a map-derived address is the source of truth
// (tcp_address+1 has no defined listener) and a legacy TCP dial already
// targets the +1 fallback listener.
func (s *TestSuite) TestShouldAttemptLegacyTCPFallback(c *C) {
	fmt.Println("Testing shouldAttemptLegacyTCPFallback gating")

	// Legacy-convention RDMA dial: primary may be unreachable over RDMA, the
	// conventional TCP fallback at +1 is defined -> allowed.
	c.Check(shouldAttemptLegacyTCPFallback(true, NvmfTransportRDMA), Equals, true)
	// Legacy-convention TCP dial already targets primary+1 -> no further fallback.
	c.Check(shouldAttemptLegacyTCPFallback(true, NvmfTransportTCP), Equals, false)
	// Map-derived dials never fall back, regardless of transport.
	c.Check(shouldAttemptLegacyTCPFallback(false, NvmfTransportRDMA), Equals, false)
	c.Check(shouldAttemptLegacyTCPFallback(false, NvmfTransportTCP), Equals, false)
}

// A remote replica base bdev can be attached over either NVMe-oF fabric
// transport: TCP (compute-node engines / the port+1 fallback) or RDMA
// (storage/RDMA-node engines dialing the replica's RDMA primary). Both must
// pass validation; only non-fabric (PCIe) or unknown transports are rejected.
// Restricting this to TCP faulted every replica on an RDMA-transport engine,
// erroring the volume on storage nodes.
func (s *TestSuite) TestValidateNvmeTransport(c *C) {
	fmt.Println("Testing validateNvmeTransport: TCP and RDMA both valid, PCIe rejected")

	info := func(tr spdktypes.NvmeTransportType, fam spdktypes.NvmeAddressFamily) spdktypes.NvmeNamespaceInfo {
		return spdktypes.NvmeNamespaceInfo{Trid: spdktypes.NvmeTransportID{Trtype: tr, Adrfam: fam}}
	}

	// Valid fabric transports.
	c.Check(validateNvmeTransport("r-1", "r-1n1", info(spdktypes.NvmeTransportTypeTCP, spdktypes.NvmeAddressFamilyIPv4)), IsNil)
	c.Check(validateNvmeTransport("r-1", "r-1n1", info(spdktypes.NvmeTransportTypeRDMA, spdktypes.NvmeAddressFamilyIPv4)), IsNil)
	c.Check(validateNvmeTransport("r-1", "r-1n1", info(spdktypes.NvmeTransportTypeRDMA, spdktypes.NvmeAddressFamilyIPv6)), IsNil)
	// Case-insensitive (SPDK may report upper-case).
	c.Check(validateNvmeTransport("r-1", "r-1n1", info(spdktypes.NvmeTransportType("RDMA"), spdktypes.NvmeAddressFamilyIPv4)), IsNil)

	// Non-fabric transport is rejected.
	c.Check(validateNvmeTransport("r-1", "r-1n1", info(spdktypes.NvmeTransportTypePCIe, spdktypes.NvmeAddressFamilyIPv4)), NotNil)
	// Valid transport but invalid address family is still rejected.
	c.Check(validateNvmeTransport("r-1", "r-1n1", info(spdktypes.NvmeTransportTypeRDMA, spdktypes.NvmeAddressFamilyIB)), NotNil)
}

func (s *TestSuite) TestEngineLegacyTransportFallback(c *C) {
	fmt.Println("Testing Engine.legacyTransportFallback (backward-compat convention)")

	// TCP engine -> primary+1 over TCP.
	tcpEng := &Engine{ReplicaTransport: NvmfTransportTCP}
	addr, trans, legacyConvention := tcpEng.legacyTransportFallback("10.10.5.19:21099")
	c.Check(addr, Equals, "10.10.5.19:21100")
	c.Check(trans, Equals, NvmfTransportTCP)
	c.Check(legacyConvention, Equals, true)

	// RDMA engine -> primary as-is over RDMA.
	rdmaEng := &Engine{ReplicaTransport: NvmfTransportRDMA}
	addr, trans, legacyConvention = rdmaEng.legacyTransportFallback("10.10.5.19:21099")
	c.Check(addr, Equals, "10.10.5.19:21099")
	c.Check(trans, Equals, NvmfTransportRDMA)
	c.Check(legacyConvention, Equals, true)

	// Unparseable address -> as-is over TCP (last resort, no crash).
	addr, trans, legacyConvention = tcpEng.legacyTransportFallback("garbage-no-port")
	c.Check(addr, Equals, "garbage-no-port")
	c.Check(trans, Equals, NvmfTransportTCP)
	c.Check(legacyConvention, Equals, true)
}

// A freshly attached rebuild-destination head must record the address and
// transport that were actually dialed: post-rebuild validation
// (validateAndUpdateReplicaNvme) compares the attached bdev against
// dialAddress(), so a status entry without DialedAddress/Transport would ERR
// a healthy, freshly rebuilt replica whose dial fell back to the TCP listener
// at primary+1.
func (s *TestSuite) TestRebuildDstReplicaStatusCarriesDialedAddress(c *C) {
	fmt.Println("Testing newRebuildDstReplicaStatus records dialed address/transport")

	const (
		canonical = "10.10.3.19:28923" // dst head primary (RDMA on RDMA storage)
		tcpFB     = "10.10.3.19:28924" // primary+1 (TCP fallback)
	)

	// A TCP engine picking the rebuild dst address for an RDMA-only-advertised
	// dst head resolves to the +1 TCP fallback per the legacy convention.
	tcpEng := &Engine{ReplicaTransport: NvmfTransportTCP}
	attachAddr, attachTransport, legacyConvention := tcpEng.pickRebuildDstAddress(canonical, &spdkrpc.ReplicaTransportAddresses{RdmaAddress: canonical})
	c.Assert(attachAddr, Equals, tcpFB)
	c.Assert(attachTransport, Equals, NvmfTransportTCP)
	c.Assert(legacyConvention, Equals, true)

	status := newRebuildDstReplicaStatus(canonical, attachAddr, attachTransport, "r-dst-1n1")
	c.Check(status.Address, Equals, canonical)
	c.Check(status.DialedAddress, Equals, tcpFB)
	c.Check(status.Transport, Equals, NvmfTransportTCP)
	c.Check(status.Mode, Equals, lhtypes.ModeWO)
	c.Check(status.BdevName, Equals, "r-dst-1n1")
	// dialAddress() (what validation compares against) must be the fallback.
	c.Check(status.dialAddress(), Equals, tcpFB)
}

// dialAddress drives replica validation/reconnect: it must return the address
// the engine actually dialed (DialedAddress, e.g. the +1 TCP fallback), not
// the canonical primary Address -- otherwise validateAndUpdateReplicaNvme
// flags a mismatch and marks the replica ERR.
func (s *TestSuite) TestEngineReplicaStatusDialAddress(c *C) {
	fmt.Println("Testing EngineReplicaStatus.dialAddress (DialedAddress preferred over Address)")

	// Fell back to the TCP fallback: DialedAddress (+1) is what was attached.
	withDialed := &EngineReplicaStatus{Address: "10.10.3.19:28929", DialedAddress: "10.10.3.19:28930"}
	c.Check(withDialed.dialAddress(), Equals, "10.10.3.19:28930")

	// No fallback (transports matched / pre-dual-listener): use canonical Address.
	noDialed := &EngineReplicaStatus{Address: "10.10.3.19:28929"}
	c.Check(noDialed.dialAddress(), Equals, "10.10.3.19:28929")

	// nil-safe.
	var nilStatus *EngineReplicaStatus
	c.Check(nilStatus.dialAddress(), Equals, "")
}
