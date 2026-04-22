package spdk

import (
	. "gopkg.in/check.v1"

	spdktypes "github.com/longhorn/go-spdk-helper/pkg/spdk/types"
)

// Pure-function tests covering the dual-listener + RDMA-transport code paths.
// No SPDK process required — all inputs are structs and the functions are
// side-effect free.

func (s *TestSuite) TestGetExposedPortForTransportPrimaryLookup(c *C) {
	// RDMA primary at 20001, TCP fallback at 20002. getExposedPortForTransport
	// must return PortStart (20001) when asked for the primary (RDMA) and
	// PortStart+1 (20002) when asked for the fallback.
	subsystem := &spdktypes.NvmfSubsystem{
		Nqn: "nqn.2023-01.io.longhorn.spdk:vol",
		ListenAddresses: []spdktypes.NvmfSubsystemListenAddress{
			{Trtype: spdktypes.NvmeTransportTypeRDMA, Adrfam: spdktypes.NvmeAddressFamilyIPv4, Traddr: "10.10.3.19", Trsvcid: "20001"},
			{Trtype: spdktypes.NvmeTransportTypeTCP, Adrfam: spdktypes.NvmeAddressFamilyIPv4, Traddr: "10.10.3.19", Trsvcid: "20002"},
		},
	}

	rdmaPort, err := getExposedPortForTransport(subsystem, spdktypes.NvmeTransportTypeRDMA)
	c.Assert(err, IsNil)
	c.Assert(rdmaPort, Equals, int32(20001))

	tcpPort, err := getExposedPortForTransport(subsystem, spdktypes.NvmeTransportTypeTCP)
	c.Assert(err, IsNil)
	c.Assert(tcpPort, Equals, int32(20002))
}

func (s *TestSuite) TestGetExposedPortSkipsNonIPv4(c *C) {
	// IPv6 listeners must be skipped — only the IPv4 entry's port is returned.
	subsystem := &spdktypes.NvmfSubsystem{
		ListenAddresses: []spdktypes.NvmfSubsystemListenAddress{
			{Trtype: spdktypes.NvmeTransportTypeTCP, Adrfam: spdktypes.NvmeAddressFamilyIPv6, Traddr: "::1", Trsvcid: "20999"},
			{Trtype: spdktypes.NvmeTransportTypeTCP, Adrfam: spdktypes.NvmeAddressFamilyIPv4, Traddr: "10.10.3.19", Trsvcid: "20001"},
		},
	}
	port, err := getExposedPortForTransport(subsystem, spdktypes.NvmeTransportTypeTCP)
	c.Assert(err, IsNil)
	c.Assert(port, Equals, int32(20001))
}

func (s *TestSuite) TestGetExposedPortMissingTransport(c *C) {
	// Only TCP listener present — asking for RDMA must error so the caller
	// can treat it as "primary not bound".
	subsystem := &spdktypes.NvmfSubsystem{
		ListenAddresses: []spdktypes.NvmfSubsystemListenAddress{
			{Trtype: spdktypes.NvmeTransportTypeTCP, Adrfam: spdktypes.NvmeAddressFamilyIPv4, Traddr: "10.10.3.19", Trsvcid: "20001"},
		},
	}
	_, err := getExposedPortForTransport(subsystem, spdktypes.NvmeTransportTypeRDMA)
	c.Assert(err, NotNil)
}

func (s *TestSuite) TestGetExposedPortNilAndEmpty(c *C) {
	_, err := getExposedPortForTransport(nil, spdktypes.NvmeTransportTypeTCP)
	c.Assert(err, NotNil)

	_, err = getExposedPortForTransport(&spdktypes.NvmfSubsystem{}, spdktypes.NvmeTransportTypeTCP)
	c.Assert(err, NotNil)
}

func (s *TestSuite) TestValidateNvmeTransportAcceptsTCPAndRDMA(c *C) {
	// Both transports must be accepted — the original pre-fix validator
	// only allowed TCP, which faulted every engine on RDMA nodes after
	// replicas came up.
	cases := []struct {
		name    string
		trtype  spdktypes.NvmeTransportType
		wantErr bool
	}{
		{"TCP is accepted", spdktypes.NvmeTransportTypeTCP, false},
		{"RDMA is accepted", spdktypes.NvmeTransportTypeRDMA, false},
		{"PCIe is rejected", spdktypes.NvmeTransportTypePCIe, true},
		{"empty is rejected", "", true},
	}
	for _, tc := range cases {
		info := spdktypes.NvmeNamespaceInfo{
			Trid: spdktypes.NvmeTransportID{
				Adrfam: spdktypes.NvmeAddressFamilyIPv4,
				Trtype: tc.trtype,
			},
		}
		err := validateNvmeTransport("r-name", "b-name", info)
		if tc.wantErr {
			c.Assert(err, NotNil, Commentf("case %q: expected error", tc.name))
		} else {
			c.Assert(err, IsNil, Commentf("case %q: expected nil error, got %v", tc.name, err))
		}
	}
}

func (s *TestSuite) TestValidateNvmeTransportRejectsNonIPv4(c *C) {
	info := spdktypes.NvmeNamespaceInfo{
		Trid: spdktypes.NvmeTransportID{
			Adrfam: spdktypes.NvmeAddressFamilyIPv6,
			Trtype: spdktypes.NvmeTransportTypeTCP,
		},
	}
	err := validateNvmeTransport("r-name", "b-name", info)
	c.Assert(err, NotNil)
}

func (s *TestSuite) TestValidateReplicaAddressAcceptsTCPFallbackPort(c *C) {
	// Primary is at ip:20001 (RDMA). If attemptTCPFallback reconnected via
	// TCP at ip:20002 (PortStart+1), the expected address still records
	// the primary; validator must accept port+1 as a valid fallback match.
	ip := "10.10.3.19"
	expected := ip + ":20001"

	fallback := spdktypes.NvmeNamespaceInfo{Trid: spdktypes.NvmeTransportID{Traddr: ip, Trsvcid: "20002"}}
	c.Assert(validateReplicaAddress("r-name", "b-name", expected, fallback), IsNil,
		Commentf("PortStart+1 TCP fallback must be treated as a valid match"))
}

func (s *TestSuite) TestValidateReplicaAddressExactPrimaryMatch(c *C) {
	// Exact match is always valid regardless of fallback logic.
	ip := "10.10.3.19"
	expected := ip + ":20001"
	actual := spdktypes.NvmeNamespaceInfo{Trid: spdktypes.NvmeTransportID{Traddr: ip, Trsvcid: "20001"}}
	c.Assert(validateReplicaAddress("r-name", "b-name", expected, actual), IsNil)
}

func (s *TestSuite) TestValidateReplicaAddressRejectsUnrelatedPort(c *C) {
	// Anything other than PortStart or PortStart+1 must fail so genuine
	// address drift still surfaces.
	ip := "10.10.3.19"
	expected := ip + ":20001"

	for _, port := range []string{"20000", "20003", "30000"} {
		actual := spdktypes.NvmeNamespaceInfo{Trid: spdktypes.NvmeTransportID{Traddr: ip, Trsvcid: port}}
		err := validateReplicaAddress("r-name", "b-name", expected, actual)
		c.Assert(err, NotNil, Commentf("port %s must not be accepted as a fallback match", port))
	}
}

func (s *TestSuite) TestValidateReplicaAddressRejectsDifferentHost(c *C) {
	// Same port, different host must still fail; address validation
	// is host-and-port, not port-only.
	expected := "10.10.3.19:20001"
	actual := spdktypes.NvmeNamespaceInfo{Trid: spdktypes.NvmeTransportID{Traddr: "10.10.3.20", Trsvcid: "20001"}}
	c.Assert(validateReplicaAddress("r-name", "b-name", expected, actual), NotNil)
}
