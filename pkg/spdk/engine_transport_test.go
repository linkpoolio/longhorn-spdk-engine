package spdk

import (
	"fmt"

	. "gopkg.in/check.v1"

	spdktypes "github.com/longhorn/go-spdk-helper/pkg/spdk/types"
	"github.com/longhorn/types/pkg/generated/spdkrpc"

	lhtypes "github.com/longhorn/longhorn-spdk-engine/pkg/types"
)

// The engine picks the address+transport to dial each replica from the typed
// transport addresses the storage IM advertises. The map is the source of
// truth: an RDMA-capable engine prefers the RDMA address; otherwise the
// advertised TCP address (on RDMA storage nodes that is the dedicated TCP
// listener at primary+1, which keeps engines on TCP-only nodes connected).
// A missing or empty entry is an error, not a case to paper over.

func (s *TestSuite) TestEnginePickReplicaAddress(c *C) {
	fmt.Println("Testing Engine.pickReplicaAddress: typed map is the source of truth; missing entries error")

	const (
		tcpAddr  = "10.10.3.19:28924" // dedicated TCP listener (primary+1 on RDMA storage)
		rdmaAddr = "10.10.3.19:28923" // RDMA primary
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
		wantErr   bool
	}{
		// No usable entry: hard error surfaced to the caller.
		{"no map entry, TCP engine -> error", NvmfTransportTCP, false, nil, "", "", true},
		{"no map entry, RDMA engine -> error", NvmfTransportRDMA, false, nil, "", "", true},
		{"nil entry -> error", NvmfTransportTCP, true, nil, "", "", true},
		{"empty entry -> error", NvmfTransportRDMA, true, empty, "", "", true},
		// Advertised both: engine transport decides.
		{"both, RDMA engine -> rdma/RDMA", NvmfTransportRDMA, true, both, rdmaAddr, NvmfTransportRDMA, false},
		{"both, TCP engine -> tcp/TCP", NvmfTransportTCP, true, both, tcpAddr, NvmfTransportTCP, false},
		// TCP-only advertised: TCP for either engine.
		{"tcp-only, RDMA engine -> tcp/TCP", NvmfTransportRDMA, true, tcpOnly, tcpAddr, NvmfTransportTCP, false},
		// RDMA-only advertised: RDMA engine uses it; a TCP engine cannot dial it.
		{"rdma-only, RDMA engine -> rdma/RDMA", NvmfTransportRDMA, true, rdmaOnly, rdmaAddr, NvmfTransportRDMA, false},
		{"rdma-only, TCP engine -> error", NvmfTransportTCP, true, rdmaOnly, "", "", true},
	}

	for _, tc := range cases {
		e := &Engine{ReplicaTransport: tc.engineTr}
		m := map[string]*spdkrpc.ReplicaTransportAddresses{}
		if tc.present {
			m["r-1"] = tc.entry
		}
		addr, trans, err := e.pickReplicaAddress("r-1", m)
		if tc.wantErr {
			c.Check(err, NotNil, Commentf("case %q: expected error", tc.name))
			continue
		}
		c.Check(err, IsNil, Commentf("case %q: unexpected error %v", tc.name, err))
		c.Check(addr, Equals, tc.wantAddr, Commentf("case %q: address", tc.name))
		c.Check(trans, Equals, tc.wantTrans, Commentf("case %q: transport", tc.name))
	}
}

// A remote replica base bdev can be attached over either NVMe-oF fabric
// transport: TCP (compute-node engines dialing the dedicated TCP listener) or
// RDMA (storage/RDMA-node engines dialing the replica's RDMA primary). Both
// must pass validation; only non-fabric (PCIe) or unknown transports are
// rejected. Restricting this to TCP faulted every replica on an RDMA-transport
// engine, erroring the volume on storage nodes.
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

// A freshly attached rebuild-destination head must record the address and
// transport that were actually dialed: post-rebuild validation
// (validateAndUpdateReplicaNvme) compares the attached bdev against
// dialAddress(), so a status entry without DialedAddress/Transport would ERR
// a healthy, freshly rebuilt replica dialed on its TCP listener.
func (s *TestSuite) TestRebuildDstReplicaStatusCarriesDialedAddress(c *C) {
	fmt.Println("Testing newRebuildDstReplicaStatus records dialed address/transport")

	const (
		canonical = "10.10.3.19:28923" // dst head primary (RDMA on RDMA storage)
		tcpAddr   = "10.10.3.19:28924" // dedicated TCP listener (primary+1)
	)

	// A TCP engine picking the rebuild dst address dials the advertised TCP
	// listener.
	tcpEng := &Engine{ReplicaTransport: NvmfTransportTCP}
	attachAddr, attachTransport, err := tcpEng.pickRebuildDstAddress(&spdkrpc.ReplicaTransportAddresses{TcpAddress: tcpAddr, RdmaAddress: canonical})
	c.Assert(err, IsNil)
	c.Assert(attachAddr, Equals, tcpAddr)
	c.Assert(attachTransport, Equals, NvmfTransportTCP)

	status := newRebuildDstReplicaStatus(canonical, attachAddr, attachTransport, "r-dst-1n1")
	c.Check(status.Address, Equals, canonical)
	c.Check(status.DialedAddress, Equals, tcpAddr)
	c.Check(status.Transport, Equals, NvmfTransportTCP)
	c.Check(status.Mode, Equals, lhtypes.ModeWO)
	c.Check(status.BdevName, Equals, "r-dst-1n1")
	// dialAddress() (what validation compares against) must be the TCP listener.
	c.Check(status.dialAddress(), Equals, tcpAddr)
}

// dialAddress drives replica validation/reconnect: it must return the address
// the engine actually dialed (DialedAddress, e.g. the dedicated TCP listener),
// not the canonical primary Address -- otherwise validateAndUpdateReplicaNvme
// flags a mismatch and marks the replica ERR.
func (s *TestSuite) TestEngineReplicaStatusDialAddress(c *C) {
	fmt.Println("Testing EngineReplicaStatus.dialAddress (DialedAddress preferred over Address)")

	// Dialed the dedicated TCP listener: DialedAddress is what was attached.
	withDialed := &EngineReplicaStatus{Address: "10.10.3.19:28929", DialedAddress: "10.10.3.19:28930"}
	c.Check(withDialed.dialAddress(), Equals, "10.10.3.19:28930")

	// Same-address dial: use canonical Address.
	noDialed := &EngineReplicaStatus{Address: "10.10.3.19:28929"}
	c.Check(noDialed.dialAddress(), Equals, "10.10.3.19:28929")

	// nil-safe.
	var nilStatus *EngineReplicaStatus
	c.Check(nilStatus.dialAddress(), Equals, "")
}
