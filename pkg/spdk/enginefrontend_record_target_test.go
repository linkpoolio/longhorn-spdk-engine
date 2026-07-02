package spdk

import (
	"fmt"

	lhtypes "github.com/longhorn/longhorn-spdk-engine/pkg/types"

	. "gopkg.in/check.v1"
)

// After an IM restart the engine is recreated on a fresh port while the
// frontend record still holds the old one; recovering or healing from that
// record dials a dead (or foreign) listener forever. The record must be
// corrected against the locally running engine before it is used.
func (s *TestSuite) TestCorrectStaleEngineFrontendRecordTarget(c *C) {
	fmt.Println("Testing stale engine frontend record target correction")

	srv := &Server{engineMap: map[string]*Engine{}}

	e := NewEngine("vol-a-e-0", "vol-a", lhtypes.FrontendEmpty, 1024, "tcp", make(chan interface{}, 16), 0)
	e.State = lhtypes.InstanceStateRunning
	e.NvmeTcpTarget.IP = "10.0.0.5"
	e.NvmeTcpTarget.Port = 20026
	e.Get() // prime the snapshot
	srv.engineMap["vol-a-e-0"] = e

	// Stale record: engine moved from 20065 to 20026.
	rec := &EngineFrontendRecord{
		Name:       "vol-a-ef-0",
		EngineName: "vol-a-e-0",
		VolumeName: "vol-a",
		Frontend:   lhtypes.FrontendEmpty,
		TargetIP:   "10.0.0.5",
		TargetPort: 20065,
		Paths: []*EngineFrontendPathRecord{
			{TargetIP: "10.0.0.5", TargetPort: 20065, EngineName: "vol-a-e-0"},
			{TargetIP: "10.0.0.9", TargetPort: 21000, EngineName: "vol-b-e-0"},
		},
	}
	c.Assert(srv.correctStaleEngineFrontendRecordTarget(rec), Equals, true)
	c.Assert(rec.TargetPort, Equals, int32(20026))
	c.Assert(rec.Paths[0].TargetPort, Equals, int32(20026))
	// A secondary path pointing at another engine is not ours to correct.
	c.Assert(rec.Paths[1].TargetPort, Equals, int32(21000))

	// Already-correct record: no correction.
	c.Assert(srv.correctStaleEngineFrontendRecordTarget(rec), Equals, false)

	// Engine not local: leave the record alone.
	remote := &EngineFrontendRecord{Name: "x-ef-0", EngineName: "x-e-0", TargetIP: "10.0.0.7", TargetPort: 30000}
	c.Assert(srv.correctStaleEngineFrontendRecordTarget(remote), Equals, false)
	c.Assert(remote.TargetPort, Equals, int32(30000))

	// Engine local but not running: leave the record alone.
	e.State = lhtypes.InstanceStateError
	e.Get()
	stale := &EngineFrontendRecord{Name: "vol-a-ef-0", EngineName: "vol-a-e-0", TargetIP: "10.0.0.5", TargetPort: 20065}
	c.Assert(srv.correctStaleEngineFrontendRecordTarget(stale), Equals, false)
}
