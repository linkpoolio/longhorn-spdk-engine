package spdk

import (
	"fmt"
	"strings"

	lhtypes "github.com/longhorn/longhorn-spdk-engine/pkg/types"

	. "gopkg.in/check.v1"
)

// newRDMAEngineFrontend builds an EngineFrontend with a single NVMe-TCP path
// whose transport is RDMA or TCP, used to exercise the switchover teardown that
// must explicitly release an old RDMA path's HCA queue pair.
func newRDMAEngineFrontend(rdmaPath bool) *EngineFrontend {
	ef := NewEngineFrontend("ef-a", "engine-a", "vol-a", lhtypes.FrontendSPDKTCPNvmf, 1024, 0, 0, make(chan interface{}, 1))
	transport := NvmfTransportTCP
	if rdmaPath {
		transport = NvmfTransportRDMA
	}
	ef.NvmeTCPPathMap["10.0.0.1:2000"] = &NvmeTCPPath{
		TargetIP:   "10.0.0.1",
		TargetPort: 2000,
		Nqn:        "nqn.test",
		Transport:  transport,
	}
	return ef
}

func (s *TestSuite) TestTeardownRemoteRDMAPathIfNeededNoopForTCPPath(c *C) {
	ef := newRDMAEngineFrontend(false)

	initiatorCalled := false
	ef.teardownRemoteRDMAPathFn = func(nqn, ip, port string) error {
		initiatorCalled = true
		return nil
	}
	listenerCalled := false
	ef.removeRemoteTargetListenerFn = func(targetIP, engineName string, transport NvmfTransportType) error {
		listenerCalled = true
		return nil
	}

	err := ef.teardownRemoteRDMAPathIfNeeded("10.0.0.1", "engine-old")
	c.Assert(err, IsNil)
	c.Assert(initiatorCalled, Equals, false)
	c.Assert(listenerCalled, Equals, false)
}

func (s *TestSuite) TestTeardownRemoteRDMAPathIfNeededDisconnectsRDMAPath(c *C) {
	ef := newRDMAEngineFrontend(true)

	var gotNQN, gotIP, gotPort string
	ef.teardownRemoteRDMAPathFn = func(nqn, ip, port string) error {
		gotNQN, gotIP, gotPort = nqn, ip, port
		return nil
	}
	var gotListenerIP, gotListenerEngine string
	var gotListenerTransport NvmfTransportType
	ef.removeRemoteTargetListenerFn = func(targetIP, engineName string, transport NvmfTransportType) error {
		gotListenerIP, gotListenerEngine, gotListenerTransport = targetIP, engineName, transport
		return nil
	}

	err := ef.teardownRemoteRDMAPathIfNeeded("10.0.0.1", "engine-old")
	c.Assert(err, IsNil)
	c.Assert(gotNQN, Equals, "nqn.test")
	c.Assert(gotIP, Equals, "10.0.0.1")
	c.Assert(gotPort, Equals, "2000")
	c.Assert(gotListenerIP, Equals, "10.0.0.1")
	c.Assert(gotListenerEngine, Equals, "engine-old")
	c.Assert(gotListenerTransport, Equals, NvmfTransportRDMA)
}

func (s *TestSuite) TestTeardownRemoteRDMAPathIfNeededNoopForMissingPath(c *C) {
	ef := newRDMAEngineFrontend(true)
	called := false
	ef.teardownRemoteRDMAPathFn = func(nqn, ip, port string) error {
		called = true
		return nil
	}
	ef.removeRemoteTargetListenerFn = func(string, string, NvmfTransportType) error {
		called = true
		return nil
	}
	err := ef.teardownRemoteRDMAPathIfNeeded("10.0.0.99", "engine-old")
	c.Assert(err, IsNil)
	c.Assert(called, Equals, false)
}

func (s *TestSuite) TestTeardownRemoteRDMAPathIfNeededEmptyIP(c *C) {
	ef := newRDMAEngineFrontend(true)
	called := false
	ef.teardownRemoteRDMAPathFn = func(nqn, ip, port string) error {
		called = true
		return nil
	}
	ef.removeRemoteTargetListenerFn = func(string, string, NvmfTransportType) error {
		called = true
		return nil
	}
	err := ef.teardownRemoteRDMAPathIfNeeded("", "engine-old")
	c.Assert(err, IsNil)
	c.Assert(called, Equals, false)
}

func (s *TestSuite) TestUpsertNVMeTCPPathSetsTransport(c *C) {
	ef := newRDMAEngineFrontend(false)

	// An explicit RDMA transport is recorded on the path so switchover can later
	// identify it as needing an explicit HCA queue-pair teardown.
	addr := ef.upsertNVMeTCPPathLocked("10.0.0.2", 3000, "engine-b", "nqn.test", "nguid", NvmeTCPANAStateOptimized, NvmfTransportRDMA)
	c.Assert(addr, Equals, "10.0.0.2:3000")
	c.Assert(ef.NvmeTCPPathMap[addr], NotNil)
	c.Assert(ef.NvmeTCPPathMap[addr].Transport, Equals, NvmfTransportRDMA)

	// An empty transport falls back to the default (TCP), which the teardown
	// treats as a no-op path.
	addr2 := ef.upsertNVMeTCPPathLocked("10.0.0.3", 4000, "engine-c", "nqn.test", "nguid", NvmeTCPANAStateOptimized, "")
	c.Assert(ef.NvmeTCPPathMap[addr2], NotNil)
	c.Assert(ef.NvmeTCPPathMap[addr2].Transport, Equals, DefaultNvmfTransport)
}

// The RDMA-specific teardown keys on the transport observed on the live
// controller (the remote listener's trtype): an observed-TCP path must be a
// no-op even when the recorded tag claims RDMA — force-disconnecting a TCP
// controller would leave an ANA rollback after a phase-3 failure with no
// path; ctrl-loss-tmo owns its cleanup.
func (s *TestSuite) TestTeardownRemoteRDMAPathNoopWhenObservedTCP(c *C) {
	ef := newRDMAEngineFrontend(true) // recorded tag says RDMA
	ef.observePathTransportFn = func(nqn, ip, port string) (NvmfTransportType, bool) {
		return NvmfTransportTCP, true // live controller is actually TCP
	}

	called := false
	ef.teardownRemoteRDMAPathFn = func(nqn, ip, port string) error {
		called = true
		return nil
	}
	ef.removeRemoteTargetListenerFn = func(string, string, NvmfTransportType) error {
		called = true
		return nil
	}

	err := ef.teardownRemoteRDMAPathIfNeeded("10.0.0.1", "engine-old")
	c.Assert(err, IsNil)
	c.Assert(called, Equals, false)
}

// Conversely, a legacy RDMA target must still get the explicit RDMA teardown
// even though new builds tag EF paths with the engine target's transport
// (TCP): the observed controller trtype wins over the recorded tag.
func (s *TestSuite) TestTeardownRemoteRDMAPathFiresWhenObservedRDMA(c *C) {
	ef := newRDMAEngineFrontend(false) // recorded tag says TCP (new tagging)
	ef.observePathTransportFn = func(nqn, ip, port string) (NvmfTransportType, bool) {
		return NvmfTransportRDMA, true // legacy target listener is RDMA
	}

	var gotNQN, gotIP, gotPort string
	ef.teardownRemoteRDMAPathFn = func(nqn, ip, port string) error {
		gotNQN, gotIP, gotPort = nqn, ip, port
		return nil
	}
	var gotListenerTransport NvmfTransportType
	ef.removeRemoteTargetListenerFn = func(targetIP, engineName string, transport NvmfTransportType) error {
		gotListenerTransport = transport
		return nil
	}

	err := ef.teardownRemoteRDMAPathIfNeeded("10.0.0.1", "engine-old")
	c.Assert(err, IsNil)
	c.Assert(gotNQN, Equals, "nqn.test")
	c.Assert(gotIP, Equals, "10.0.0.1")
	c.Assert(gotPort, Equals, "2000")
	c.Assert(gotListenerTransport, Equals, NvmfTransportRDMA)
}

// When the controller is unobservable (already disconnected, transient
// nvme-cli failure) the teardown falls back to the recorded path tag.
func (s *TestSuite) TestTeardownRemoteRDMAPathFallsBackToRecordedTag(c *C) {
	ef := newRDMAEngineFrontend(true)
	ef.observePathTransportFn = func(nqn, ip, port string) (NvmfTransportType, bool) {
		return "", false // unobservable
	}
	called := false
	ef.teardownRemoteRDMAPathFn = func(nqn, ip, port string) error {
		called = true
		return nil
	}
	ef.removeRemoteTargetListenerFn = func(string, string, NvmfTransportType) error { return nil }

	err := ef.teardownRemoteRDMAPathIfNeeded("10.0.0.1", "engine-old")
	c.Assert(err, IsNil)
	c.Assert(called, Equals, true)
}

// The EF path transport tag must be derived from the engine target's actual
// transport — the listener the kernel initiator dials is pinned to TCP in
// createNVMeTCPTarget — not from the node's negotiated transport.
func (s *TestSuite) TestEngineFrontendPathTransportDerivation(c *C) {
	c.Check(engineFrontendTargetTransport(), Equals, NvmfTransportTCP)

	// The engine's own target transport agrees with the shared derivation.
	e := &Engine{NvmeTcpTarget: &NvmeTcpTarget{Transport: engineFrontendTargetTransport()}}
	c.Check(e.targetTransport(), Equals, engineFrontendTargetTransport())

	// Paths tagged through syncCurrentNVMeTCPPathLocked inherit the tag.
	ef := NewEngineFrontend("ef-t", "engine-t", "vol-t", lhtypes.FrontendSPDKTCPNvmf, 1024, 0, 0, make(chan interface{}, 1))
	ef.NvmeTcpFrontend.Transport = engineFrontendTargetTransport()
	ef.NvmeTcpFrontend.TargetIP = "10.0.0.5"
	ef.NvmeTcpFrontend.TargetPort = 2100
	ef.syncCurrentNVMeTCPPathLocked()
	path := ef.NvmeTCPPathMap["10.0.0.5:2100"]
	c.Assert(path, NotNil)
	c.Check(path.Transport, Equals, NvmfTransportTCP)
}

func (s *TestSuite) TestTeardownRemoteRDMAPathIfNeededAggregatesErrors(c *C) {
	ef := newRDMAEngineFrontend(true)
	ef.teardownRemoteRDMAPathFn = func(nqn, ip, port string) error {
		return fmt.Errorf("qp teardown failed")
	}
	ef.removeRemoteTargetListenerFn = func(string, string, NvmfTransportType) error {
		return fmt.Errorf("listener removal failed")
	}
	err := ef.teardownRemoteRDMAPathIfNeeded("10.0.0.1", "engine-old")
	c.Assert(err, NotNil)
	c.Assert(strings.Contains(err.Error(), "qp teardown failed"), Equals, true)
	c.Assert(strings.Contains(err.Error(), "listener removal failed"), Equals, true)
}

// The per-path transport must reach the proto report: the manager publishes it
// on the EngineFrontend CRD status, and operators key on it to see whether a
// frontend path is attached over TCP or RDMA.
func (s *TestSuite) TestProtoNvmeTCPPathsCarryTransport(c *C) {
	for _, rdma := range []bool{false, true} {
		ef := newRDMAEngineFrontend(rdma)
		paths := ef.getProtoNvmeTCPPathsWithoutLock()
		c.Assert(paths, HasLen, 1)
		want := string(NvmfTransportTCP)
		if rdma {
			want = string(NvmfTransportRDMA)
		}
		c.Check(paths[0].Transport, Equals, want)
		c.Check(paths[0].AnaState, Equals, string(ef.NvmeTCPPathMap["10.0.0.1:2000"].ANAState))
	}
}
