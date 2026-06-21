package spdk

import (
	"fmt"
	"os"
	"path/filepath"

	lhtypes "github.com/longhorn/longhorn-spdk-engine/pkg/types"

	. "gopkg.in/check.v1"
)

// deriveLiveState is the pure heart of the EngineFrontend derived-state
// migration: it maps a persisted record + raw host observation onto the
// canonical Running/Stopped/Error view that gRPC handlers and the self-heal
// reconciler consume. It has no I/O, so it is fully table-testable here. The
// status-sync regression that took down prod volumes lived precisely in this
// mapping (a physically-up blockdev observed with all layers absent derives
// Stopped, which the reconciler silently skips), so these cases pin every
// blockdev layer combination plus the nvmf / empty / unknown arms.

type deriveLiveStateCase struct {
	name string

	frontend string
	record   *EngineFrontendRecord
	raw      *EngineFrontendObservedRaw

	// wantState is the untyped state string constant (e.g.
	// lhtypes.InstanceStateRunning); compared against string(got.State)
	// since gocheck's Equals is type-strict and State is typed InstanceState.
	wantState    string
	wantEndpoint string
	// wantErrMsg true means ErrorMsg must be non-empty (the breadcrumb /
	// describePartialState text is asserted loosely, not verbatim).
	wantErrMsg bool
}

func (s *TestSuite) TestDeriveLiveStateBlockdev(c *C) {
	fmt.Println("Testing deriveLiveState across all FrontendSPDKTCPBlockdev layer combinations")

	const (
		vol    = "vol-blockdev"
		nqn    = "nqn.2014-08.org.nvmexpress:uuid:vol-blockdev"
		devPth = "/dev/longhorn/vol-blockdev"
	)
	baseRecord := &EngineFrontendRecord{
		Name:       "ef-blockdev",
		VolumeName: vol,
		VolumeNQN:  nqn,
		Frontend:   lhtypes.FrontendSPDKTCPBlockdev,
	}

	// raw builder: the four blockdev layers + kernel controller state.
	raw := func(subsystem, kernel, dm, devpath bool, kstate KernelControllerState) *EngineFrontendObservedRaw {
		return &EngineFrontendObservedRaw{
			SubsystemPresent:        subsystem,
			KernelControllerPresent: kernel,
			KernelControllerState:   kstate,
			DMDevicePresent:         dm,
			DevicePathExists:        devpath,
			DevicePath:              devPth,
		}
	}

	cases := []deriveLiveStateCase{
		{
			// All four layers absent: the legitimate pre-create state. This
			// is also the exact shape the status-sync regression produced for
			// a frontend that was actually up — Stopped, non-Error, silently
			// skipped by the reconciler. deriveLiveState is correct here; the
			// bug was upstream in observation, which the new Debug log surfaces.
			name:      "all layers absent -> Stopped",
			raw:       raw(false, false, false, false, KernelControllerStateAbsent),
			wantState: lhtypes.InstanceStateStopped,
		},
		{
			name:         "all layers present + kernel live -> Running",
			raw:          raw(true, true, true, true, KernelControllerStateLive),
			wantState:    lhtypes.InstanceStateRunning,
			wantEndpoint: devPth,
		},
		{
			// Kernel mid-recovery (connecting/resetting): kernel queues I/O
			// and will self-heal or go dead. Tearing host state down here
			// would race the kernel and corrupt a mounted fs, so this MUST
			// report Running with a breadcrumb, not Error.
			name:         "all layers present + kernel transient -> Running (breadcrumb, no heal)",
			raw:          raw(true, true, true, true, KernelControllerStateTransient),
			wantState:    lhtypes.InstanceStateRunning,
			wantEndpoint: devPth,
			wantErrMsg:   true,
		},
		{
			name:       "all layers present + kernel dead -> Error",
			raw:        raw(true, true, true, true, KernelControllerStateDead),
			wantState:  lhtypes.InstanceStateError,
			wantErrMsg: true,
		},
		{
			name:       "all layers present + kernel absent -> Error",
			raw:        raw(true, true, true, true, KernelControllerStateAbsent),
			wantState:  lhtypes.InstanceStateError,
			wantErrMsg: true,
		},
		{
			// Subsystem up but the rest of the stack never came up: torn.
			name:       "only subsystem present -> Error (partial)",
			raw:        raw(true, false, false, false, KernelControllerStateAbsent),
			wantState:  lhtypes.InstanceStateError,
			wantErrMsg: true,
		},
		{
			// Device file gone but SPDK + kernel + dm still think they are up.
			name:       "subsystem+kernel+dm present, device path missing -> Error (partial)",
			raw:        raw(true, true, true, false, KernelControllerStateLive),
			wantState:  lhtypes.InstanceStateError,
			wantErrMsg: true,
		},
		{
			// SPDK subsystem torn down underneath a still-present host stack.
			name:       "subsystem absent, host stack present -> Error (partial)",
			raw:        raw(false, true, true, true, KernelControllerStateLive),
			wantState:  lhtypes.InstanceStateError,
			wantErrMsg: true,
		},
	}

	for _, tc := range cases {
		fmt.Printf("  case: %s\n", tc.name)
		got := deriveLiveState(baseRecord, tc.raw)
		c.Assert(got, NotNil, Commentf("case %q", tc.name))
		c.Check(string(got.State), Equals, tc.wantState, Commentf("case %q: state", tc.name))
		c.Check(got.Endpoint, Equals, tc.wantEndpoint, Commentf("case %q: endpoint", tc.name))
		if tc.wantErrMsg {
			c.Check(got.ErrorMsg, Not(Equals), "", Commentf("case %q: expected non-empty ErrorMsg", tc.name))
		} else {
			c.Check(got.ErrorMsg, Equals, "", Commentf("case %q: expected empty ErrorMsg", tc.name))
		}
		// Record passthrough is part of the contract — callers read it back.
		c.Check(got.Record, Equals, baseRecord, Commentf("case %q: record passthrough", tc.name))
	}
}

func (s *TestSuite) TestDeriveLiveStateNvmf(c *C) {
	fmt.Println("Testing deriveLiveState for FrontendSPDKTCPNvmf (listener-only state)")

	const (
		nqn  = "nqn.2014-08.org.nvmexpress:uuid:vol-nvmf"
		ip   = "10.0.0.5"
		port = int32(9503)
	)
	record := &EngineFrontendRecord{
		Name:       "ef-nvmf",
		VolumeName: "vol-nvmf",
		VolumeNQN:  nqn,
		Frontend:   lhtypes.FrontendSPDKTCPNvmf,
		TargetIP:   ip,
		TargetPort: port,
	}

	// Subsystem present -> Running, endpoint is the nvmf URL built from the
	// observed listener address.
	rawUp := &EngineFrontendObservedRaw{
		SubsystemPresent: true,
		NvmfTargetIP:     ip,
		NvmfTargetPort:   port,
	}
	gotUp := deriveLiveState(record, rawUp)
	c.Check(string(gotUp.State), Equals, lhtypes.InstanceStateRunning)
	c.Check(gotUp.Endpoint, Equals, GetNvmfEndpoint(nqn, ip, port))
	c.Check(gotUp.ErrorMsg, Equals, "")

	// Subsystem absent -> Stopped, no endpoint. Kernel/dm layers are
	// irrelevant for nvmf and must not affect the outcome.
	rawDown := &EngineFrontendObservedRaw{
		SubsystemPresent:        false,
		KernelControllerPresent: true,
		DMDevicePresent:         true,
		DevicePathExists:        true,
	}
	gotDown := deriveLiveState(record, rawDown)
	c.Check(string(gotDown.State), Equals, lhtypes.InstanceStateStopped)
	c.Check(gotDown.Endpoint, Equals, "")
	c.Check(gotDown.ErrorMsg, Equals, "")
}

func (s *TestSuite) TestDeriveLiveStateEmptyAndUnknown(c *C) {
	fmt.Println("Testing deriveLiveState for FrontendEmpty (always Running) and unknown frontend (Error)")

	// FrontendEmpty has no host-side surface to observe: always Running,
	// regardless of what the raw observation happens to contain.
	empty := &EngineFrontendRecord{
		Name:       "ef-empty",
		VolumeName: "vol-empty",
		Frontend:   lhtypes.FrontendEmpty,
	}
	gotEmpty := deriveLiveState(empty, &EngineFrontendObservedRaw{})
	c.Check(string(gotEmpty.State), Equals, lhtypes.InstanceStateRunning)
	c.Check(gotEmpty.Endpoint, Equals, "")
	c.Check(gotEmpty.ErrorMsg, Equals, "")

	// Empty must stay Running even with a fully-present raw (defensive: the
	// observer skips probes for empty, but the pure function must not depend
	// on that).
	gotEmptyNoisy := deriveLiveState(empty, &EngineFrontendObservedRaw{
		SubsystemPresent:        true,
		KernelControllerPresent: true,
	})
	c.Check(string(gotEmptyNoisy.State), Equals, lhtypes.InstanceStateRunning)

	// An unrecognised frontend string is a programming error / corrupt
	// record — Error with a descriptive message, never silently Running.
	unknown := &EngineFrontendRecord{
		Name:       "ef-unknown",
		VolumeName: "vol-unknown",
		Frontend:   "totally-bogus-frontend",
	}
	gotUnknown := deriveLiveState(unknown, &EngineFrontendObservedRaw{})
	c.Check(string(gotUnknown.State), Equals, lhtypes.InstanceStateError)
	c.Check(gotUnknown.ErrorMsg, Not(Equals), "")
}

// TestDeriveLiveStateErrorNeverHasEndpoint locks in the invariant that the
// heal-deferral consumer guard in reconcileOnce depends on: deriveLiveState
// only populates Endpoint for Running states, so every Error state — the only
// kind that reaches the heal block — has an empty Endpoint.
//
// The original guard keyed on `live.Endpoint != ""`, which made it dead code:
// it could never be true at heal time, so heal tore down dm-linear +
// /dev/longhorn/<vol> WITHOUT checking for a mounted filesystem. This is the
// over-eager-heal corruption path. The fix derives the device path from the
// record instead; this test guarantees nobody reintroduces an Endpoint-keyed
// guard by making the underlying assumption explicit and enforced.
func (s *TestSuite) TestDeriveLiveStateErrorNeverHasEndpoint(c *C) {
	fmt.Println("Testing the heal-guard invariant: every Error-derived state has an empty Endpoint")

	record := &EngineFrontendRecord{
		Name:       "ef-guard",
		VolumeName: "vol-guard",
		VolumeNQN:  "nqn.2014-08.org.nvmexpress:uuid:vol-guard",
		Frontend:   lhtypes.FrontendSPDKTCPBlockdev,
	}

	// Enumerate every blockdev layer bitmap (0b0000..0b1111) crossed with
	// every kernel controller state. Any combination that derives Error MUST
	// have an empty Endpoint, otherwise the (now record-derived) guard could
	// be tempted back onto live.Endpoint and silently break again.
	kstates := []KernelControllerState{
		KernelControllerStateAbsent,
		KernelControllerStateLive,
		KernelControllerStateTransient,
		KernelControllerStateDead,
	}
	sawError := false
	for bitmap := 0; bitmap < 16; bitmap++ {
		for _, ks := range kstates {
			raw := &EngineFrontendObservedRaw{
				SubsystemPresent:        bitmap&0b1000 != 0,
				KernelControllerPresent: bitmap&0b0100 != 0,
				DMDevicePresent:         bitmap&0b0010 != 0,
				DevicePathExists:        bitmap&0b0001 != 0,
				KernelControllerState:   ks,
				DevicePath:              "/dev/longhorn/vol-guard",
			}
			got := deriveLiveState(record, raw)
			if got.State == lhtypes.InstanceStateError {
				sawError = true
				c.Check(got.Endpoint, Equals, "",
					Commentf("bitmap=%04b kstate=%s derived Error but has a non-empty Endpoint", bitmap, ks))
			}
		}
	}
	// Sanity: the enumeration must actually exercise the Error branch, else
	// the invariant above is vacuously true and protects nothing.
	c.Assert(sawError, Equals, true)
}

func (s *TestSuite) TestBoolsToBitmap(c *C) {
	fmt.Println("Testing boolsToBitmap ordering (MSB first) — underpins the blockdev layer switch")

	c.Check(boolsToBitmap(false, false, false, false), Equals, 0b0000)
	c.Check(boolsToBitmap(true, true, true, true), Equals, 0b1111)
	c.Check(boolsToBitmap(true, false, false, false), Equals, 0b1000)
	c.Check(boolsToBitmap(false, false, false, true), Equals, 0b0001)
	c.Check(boolsToBitmap(true, false, true, false), Equals, 0b1010)
}

// dmLinearIsLive is the dm-linear liveness probe that keeps the observer from
// reporting a stale-positive Running for a dead-but-present device file (which
// would pin reconcileOnce's heal counter at zero). os.Stat success is not
// enough — only a real open distinguishes a live mapping from an orphaned mknod
// inode. A live device opens cleanly (/dev/null stands in as a real char
// device); a regular file is rejected by the ModeDevice guard; a missing path
// returns false.
func (s *TestSuite) TestDmLinearIsLive(c *C) {
	fmt.Println("Testing dmLinearIsLive: device opens => true; regular file / missing => false")

	// A live device node opens cleanly. /dev/null is a real char device
	// present in every linux test environment and carries ModeDevice.
	c.Check(dmLinearIsLive("/dev/null"), Equals, true)

	// A regular file exists and stats fine but is not a device — the
	// ModeDevice guard must reject it (this is the stray-file case the bare
	// os.Stat used to mis-classify as present).
	regular := filepath.Join(c.MkDir(), "not-a-device")
	c.Assert(os.WriteFile(regular, []byte("x"), 0600), IsNil)
	c.Check(dmLinearIsLive(regular), Equals, false)

	// A missing path returns false rather than erroring.
	c.Check(dmLinearIsLive(filepath.Join(c.MkDir(), "absent")), Equals, false)
}

// deviceReadsLive is the heal-time discriminator that replaced the presence-based
// devicePathInUse guard (which deferred heal forever on a stale EIO mount — the
// mode-J EF-stale-positive). The reconciler heals UNLESS the device is confirmed
// live, so this must return true ONLY for a present device that actually serves a
// read, and false for absent / non-device / unreadable paths.
func (s *TestSuite) TestDeviceReadsLive(c *C) {
	fmt.Println("Testing deviceReadsLive: present readable device => true; missing / regular file => false")

	// /dev/zero is a live char device that returns data on read in every linux
	// test env — confirmed live, so heal would DEFER (don't touch a live volume).
	c.Check(deviceReadsLive("/dev/zero"), Equals, true)

	// A regular file is not a device (ModeDevice guard) — heal proceeds.
	regular := filepath.Join(c.MkDir(), "not-a-device")
	c.Assert(os.WriteFile(regular, []byte("x"), 0600), IsNil)
	c.Check(deviceReadsLive(regular), Equals, false)

	// A missing device path (e.g. /dev/longhorn/<vol> gone after rollout) is
	// not live — heal proceeds to rebuild it.
	c.Check(deviceReadsLive(filepath.Join(c.MkDir(), "absent")), Equals, false)
}

// Heal must not race a deletion: Delete closes stopCh and marks the EF
// Terminating, and a heal firing in that window would resurrect host state
// (kernel NVMe session, dm device) for a frontend the manager is tearing
// down. The guard mirrors the existing isCreating/isSwitchingOver/isExpanding
// checks.
func (s *TestSuite) TestEngineFrontendHealSkipsDuringDeletion(c *C) {
	fmt.Println("Testing EngineFrontend.Heal deletion guard")

	record := &EngineFrontendRecord{
		Name:       "ef-del",
		VolumeName: "vol-del",
		Frontend:   lhtypes.FrontendSPDKTCPBlockdev,
		TargetIP:   "10.0.0.7",
		TargetPort: 2100,
	}

	// stopCh closed (Delete ran or is running) -> Heal must back off without
	// touching state or attempting a Create.
	ef := NewEngineFrontend("ef-del", "engine-del", "vol-del", lhtypes.FrontendSPDKTCPBlockdev, 1024, 0, 0, make(chan interface{}, 1))
	ef.State = lhtypes.InstanceStateRunning
	close(ef.stopCh)
	c.Assert(ef.Heal(nil, record), IsNil)
	c.Check(string(ef.State), Equals, lhtypes.InstanceStateRunning) // not reset to Pending

	// Terminating state (Delete's defer already ran) -> same back-off.
	ef2 := NewEngineFrontend("ef-del2", "engine-del2", "vol-del", lhtypes.FrontendSPDKTCPBlockdev, 1024, 0, 0, make(chan interface{}, 1))
	ef2.State = lhtypes.InstanceStateTerminating
	c.Assert(ef2.Heal(nil, record), IsNil)
	c.Check(string(ef2.State), Equals, lhtypes.InstanceStateTerminating)
}
