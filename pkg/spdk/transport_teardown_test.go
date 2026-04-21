package spdk

import (
	"fmt"

	lhtypes "github.com/longhorn/longhorn-spdk-engine/pkg/types"

	. "gopkg.in/check.v1"
)

func (s *TestSuite) TestTeardownRemoteRDMAPathIfNeededNoopForTCPPath(c *C) {
	ef := NewEngineFrontend("ef-a", "engine-a", "vol-a", lhtypes.FrontendSPDKTCPNvmf, 1024, 0, 0, make(chan interface{}, 1))
	ef.NvmeTCPPathMap["10.0.0.1:2000"] = &NvmeTCPPath{
		TargetIP:   "10.0.0.1",
		TargetPort: 2000,
		Nqn:        "nqn.test",
		Transport:  NvmfTransportTCP,
	}

	called := false
	ef.teardownRemoteRDMAPathFn = func(nqn, ip, port string) error {
		called = true
		return nil
	}

	err := ef.teardownRemoteRDMAPathIfNeeded("10.0.0.1")
	c.Assert(err, IsNil)
	c.Assert(called, Equals, false)
}

func (s *TestSuite) TestTeardownRemoteRDMAPathIfNeededDisconnectsRDMAPath(c *C) {
	ef := NewEngineFrontend("ef-a", "engine-a", "vol-a", lhtypes.FrontendSPDKTCPNvmf, 1024, 0, 0, make(chan interface{}, 1))
	ef.NvmeTCPPathMap["10.0.0.1:2000"] = &NvmeTCPPath{
		TargetIP:   "10.0.0.1",
		TargetPort: 2000,
		Nqn:        "nqn.test",
		Transport:  NvmfTransportRDMA,
	}

	var gotNQN, gotIP, gotPort string
	ef.teardownRemoteRDMAPathFn = func(nqn, ip, port string) error {
		gotNQN = nqn
		gotIP = ip
		gotPort = port
		return nil
	}

	err := ef.teardownRemoteRDMAPathIfNeeded("10.0.0.1")
	c.Assert(err, IsNil)
	c.Assert(gotNQN, Equals, "nqn.test")
	c.Assert(gotIP, Equals, "10.0.0.1")
	c.Assert(gotPort, Equals, "2000")
}

func (s *TestSuite) TestTeardownRemoteRDMAPathIfNeededNoopForMissingPath(c *C) {
	ef := NewEngineFrontend("ef-a", "engine-a", "vol-a", lhtypes.FrontendSPDKTCPNvmf, 1024, 0, 0, make(chan interface{}, 1))
	called := false
	ef.teardownRemoteRDMAPathFn = func(nqn, ip, port string) error {
		called = true
		return nil
	}
	err := ef.teardownRemoteRDMAPathIfNeeded("10.0.0.99")
	c.Assert(err, IsNil)
	c.Assert(called, Equals, false)
}

func (s *TestSuite) TestTeardownRemoteRDMAPathIfNeededEmptyIP(c *C) {
	ef := NewEngineFrontend("ef-a", "engine-a", "vol-a", lhtypes.FrontendSPDKTCPNvmf, 1024, 0, 0, make(chan interface{}, 1))
	called := false
	ef.teardownRemoteRDMAPathFn = func(nqn, ip, port string) error {
		called = true
		return nil
	}
	err := ef.teardownRemoteRDMAPathIfNeeded("")
	c.Assert(err, IsNil)
	c.Assert(called, Equals, false)
}

func (s *TestSuite) TestTeardownRemoteRDMAPathIfNeededPropagatesError(c *C) {
	ef := NewEngineFrontend("ef-a", "engine-a", "vol-a", lhtypes.FrontendSPDKTCPNvmf, 1024, 0, 0, make(chan interface{}, 1))
	ef.NvmeTCPPathMap["10.0.0.1:2000"] = &NvmeTCPPath{
		TargetIP:   "10.0.0.1",
		TargetPort: 2000,
		Nqn:        "nqn.test",
		Transport:  NvmfTransportRDMA,
	}
	ef.teardownRemoteRDMAPathFn = func(nqn, ip, port string) error {
		return fmt.Errorf("qp teardown failed")
	}
	err := ef.teardownRemoteRDMAPathIfNeeded("10.0.0.1")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "qp teardown failed")
}
