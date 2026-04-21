package spdk

import (
	"fmt"
	"strings"

	lhtypes "github.com/longhorn/longhorn-spdk-engine/pkg/types"

	. "gopkg.in/check.v1"
)

func newRDMAEngineFrontend(rdmaPath bool) *EngineFrontend {
	ef := NewEngineFrontend("ef-a", "engine-a", "vol-a", lhtypes.FrontendSPDKTCPNvmf, 1024, 0, 0, NvmfTransportTCP, make(chan interface{}, 1))
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
