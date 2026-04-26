package spdk

import (
	"strings"

	. "gopkg.in/check.v1"

	"github.com/longhorn/longhorn-spdk-engine/pkg/types"
)

func (s *TestSuite) TestDeriveLiveStateEmptyFrontendAlwaysRunning(c *C) {
	rec := &EngineFrontendRecord{Name: "ef-1", Frontend: types.FrontendEmpty}
	live := deriveLiveState(rec, &EngineFrontendObservedRaw{})
	c.Check(live.State, Equals, types.InstanceState(types.InstanceStateRunning))
	c.Check(live.Endpoint, Equals, "")
	c.Check(live.Record, Equals, rec)
}

func (s *TestSuite) TestDeriveLiveStateNvmfSubsystemPresent(c *C) {
	rec := &EngineFrontendRecord{
		Name:      "ef-1",
		Frontend:  types.FrontendSPDKTCPNvmf,
		VolumeNQN: "nqn.2023-01.io.longhorn.spdk:vol-a",
	}
	raw := &EngineFrontendObservedRaw{
		SubsystemPresent: true,
		NvmfTargetIP:     "10.0.0.5",
		NvmfTargetPort:   20001,
	}
	live := deriveLiveState(rec, raw)
	c.Check(live.State, Equals, types.InstanceState(types.InstanceStateRunning))
	c.Check(live.Endpoint, Not(Equals), "")
	c.Check(strings.Contains(live.Endpoint, "10.0.0.5"), Equals, true)
}

func (s *TestSuite) TestDeriveLiveStateNvmfSubsystemAbsent(c *C) {
	rec := &EngineFrontendRecord{
		Name:     "ef-1",
		Frontend: types.FrontendSPDKTCPNvmf,
	}
	live := deriveLiveState(rec, &EngineFrontendObservedRaw{SubsystemPresent: false})
	c.Check(live.State, Equals, types.InstanceState(types.InstanceStateStopped))
	c.Check(live.Endpoint, Equals, "")
}

func (s *TestSuite) TestDeriveLiveStateBlockdevAllPresentAndLive(c *C) {
	rec := &EngineFrontendRecord{Name: "ef-1", Frontend: types.FrontendSPDKTCPBlockdev}
	raw := &EngineFrontendObservedRaw{
		SubsystemPresent:        true,
		KernelControllerPresent: true,
		KernelControllerLive:    true,
		DMDevicePresent:         true,
		DevicePathExists:        true,
		DevicePath:              "/dev/longhorn/pvc-58a5db03",
	}
	live := deriveLiveState(rec, raw)
	c.Check(live.State, Equals, types.InstanceState(types.InstanceStateRunning))
	c.Check(live.Endpoint, Equals, "/dev/longhorn/pvc-58a5db03")
	c.Check(live.ErrorMsg, Equals, "")
}

func (s *TestSuite) TestDeriveLiveStateBlockdevAllAbsentIsStopped(c *C) {
	rec := &EngineFrontendRecord{Name: "ef-1", Frontend: types.FrontendSPDKTCPBlockdev}
	live := deriveLiveState(rec, &EngineFrontendObservedRaw{})
	c.Check(live.State, Equals, types.InstanceState(types.InstanceStateStopped))
	c.Check(live.Endpoint, Equals, "")
}

func (s *TestSuite) TestDeriveLiveStateBlockdevControllerNotLive(c *C) {
	// All four layers present but kernel controller is in
	// connecting/resetting/failed state. The bug behind this is what bit
	// windrose 2026-04-25: dm exists, /dev/longhorn/<pvc> exists, ctrlr
	// session exists but kernel reports it as not live → I/O fails. Map to
	// Error so the reconciler tears down + recreates.
	rec := &EngineFrontendRecord{Name: "ef-1", Frontend: types.FrontendSPDKTCPBlockdev}
	raw := &EngineFrontendObservedRaw{
		SubsystemPresent:        true,
		KernelControllerPresent: true,
		KernelControllerLive:    false,
		DMDevicePresent:         true,
		DevicePathExists:        true,
	}
	live := deriveLiveState(rec, raw)
	c.Check(live.State, Equals, types.InstanceState(types.InstanceStateError))
	c.Check(strings.Contains(live.ErrorMsg, "not in live state"), Equals, true)
}

func (s *TestSuite) TestDeriveLiveStateBlockdevPartialDmMissing(c *C) {
	// SPDK + kernel ctrlr present, but dm-linear was torn down externally
	// (e.g. operator dmsetup remove). Record says we should be serving
	// but host doesn't have the device exposed.
	rec := &EngineFrontendRecord{Name: "ef-1", Frontend: types.FrontendSPDKTCPBlockdev}
	raw := &EngineFrontendObservedRaw{
		SubsystemPresent:        true,
		KernelControllerPresent: true,
		KernelControllerLive:    true,
		DMDevicePresent:         false,
		DevicePathExists:        false,
	}
	live := deriveLiveState(rec, raw)
	c.Check(live.State, Equals, types.InstanceState(types.InstanceStateError))
	c.Check(strings.Contains(live.ErrorMsg, "dm-linear"), Equals, true)
}

func (s *TestSuite) TestDeriveLiveStateBlockdevPartialKernelMissing(c *C) {
	// SPDK + dm present but kernel session vanished (NVMe controller got
	// removed, perhaps by a stray disconnect). dm-linear now points at a
	// dead device. Recorded windrose-flavoured failure mode.
	rec := &EngineFrontendRecord{Name: "ef-1", Frontend: types.FrontendSPDKTCPBlockdev}
	raw := &EngineFrontendObservedRaw{
		SubsystemPresent:        true,
		KernelControllerPresent: false,
		DMDevicePresent:         true,
		DevicePathExists:        true,
	}
	live := deriveLiveState(rec, raw)
	c.Check(live.State, Equals, types.InstanceState(types.InstanceStateError))
	c.Check(strings.Contains(live.ErrorMsg, "kernel-nvme-ctrlr"), Equals, true)
}

func (s *TestSuite) TestDeriveLiveStateBlockdevPartialSubsystemMissing(c *C) {
	// Engine target side gone (SPDK restart with stale persisted record),
	// but kernel + dm still hanging around. Bdev_nvme would be in
	// reconnecting state. Reconciler should tear down kernel + dm and
	// recreate when subsystem comes back.
	rec := &EngineFrontendRecord{Name: "ef-1", Frontend: types.FrontendSPDKTCPBlockdev}
	raw := &EngineFrontendObservedRaw{
		SubsystemPresent:        false,
		KernelControllerPresent: true,
		KernelControllerLive:    false,
		DMDevicePresent:         true,
		DevicePathExists:        true,
	}
	live := deriveLiveState(rec, raw)
	c.Check(live.State, Equals, types.InstanceState(types.InstanceStateError))
	c.Check(strings.Contains(live.ErrorMsg, "spdk-subsystem"), Equals, true)
}

func (s *TestSuite) TestDeriveLiveStateUnknownFrontendIsError(c *C) {
	rec := &EngineFrontendRecord{Name: "ef-1", Frontend: "bogus"}
	live := deriveLiveState(rec, &EngineFrontendObservedRaw{})
	c.Check(live.State, Equals, types.InstanceState(types.InstanceStateError))
	c.Check(strings.Contains(live.ErrorMsg, "unknown frontend type"), Equals, true)
}

func (s *TestSuite) TestBoolsToBitmap(c *C) {
	c.Check(boolsToBitmap(), Equals, 0)
	c.Check(boolsToBitmap(false), Equals, 0b0)
	c.Check(boolsToBitmap(true), Equals, 0b1)
	c.Check(boolsToBitmap(true, false), Equals, 0b10)
	c.Check(boolsToBitmap(true, false, true, true), Equals, 0b1011)
	c.Check(boolsToBitmap(true, true, true, true), Equals, 0b1111)
}
