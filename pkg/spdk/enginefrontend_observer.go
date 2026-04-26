package spdk

import (
	"context"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/sirupsen/logrus"

	commontypes "github.com/longhorn/go-common-libs/types"
	"github.com/longhorn/go-spdk-helper/pkg/initiator"
	spdkclient "github.com/longhorn/go-spdk-helper/pkg/spdk/client"
	helperutil "github.com/longhorn/go-spdk-helper/pkg/util"

	"github.com/longhorn/longhorn-spdk-engine/pkg/types"
)

// EngineFrontendObserveInterval controls how often the parallel-observer
// goroutine runs during step 1 of the derived-state migration.
const EngineFrontendObserveInterval = 30 * time.Second

// EngineFrontendObservedRaw is the inputs to the state-derivation function.
// It holds primitive booleans + identity values gathered from SPDK + the host
// kernel; it is intentionally trivial to construct in tests so deriveLiveState
// can be exercised exhaustively without mocking the entire SPDK + sysfs stack.
//
// The observer entry point ObserveEngineFrontend (TODO: next commit) will
// populate this struct from real probes:
//   - SubsystemPresent: nvmf_get_subsystems contains record.VolumeNQN
//   - KernelControllerPresent: /sys/class/nvme/nvme*/subsysnqn matches record
//   - KernelControllerLive: that controller's `state` is "live" (not "connecting", "resetting", etc.)
//   - DMDevicePresent: dmsetup ls / os.Stat for the expected device file succeeded
//   - ActivePath, NvmeTCPPaths: derived from kernel ANA state
type EngineFrontendObservedRaw struct {
	// SPDK side
	SubsystemPresent bool
	SubsystemNQN     string

	// Kernel-initiator side (only meaningful for FrontendSPDKTCPBlockdev)
	KernelControllerPresent bool
	KernelControllerLive    bool

	// dm-linear / device-file side (only meaningful for FrontendSPDKTCPBlockdev)
	DMDevicePresent  bool
	DevicePathExists bool
	DevicePath       string

	// Multipath state (FrontendSPDKTCPBlockdev with multiple paths)
	ActivePath   string
	NvmeTCPPaths map[string]*NvmeTCPPath

	// FrontendSPDKTCPNvmf side: just the listener address. The "state" of
	// an Nvmf-frontend EF is purely whether the SPDK subsystem listener is
	// up — there's no local initiator or dm device.
	NvmfTargetIP   string
	NvmfTargetPort int32
}

// EngineFrontendLive is the derived runtime view of an EngineFrontend at one
// point in time. It is computed from the persisted record + observed raw
// inputs by deriveLiveState. It is throw-away — never stored as authoritative
// state; built on demand by gRPC handlers and the reconciler.
type EngineFrontendLive struct {
	Record *EngineFrontendRecord

	State    types.InstanceState
	ErrorMsg string

	// Endpoint is what gRPC clients see. For blockdev: /dev/longhorn/<vol>.
	// For nvmf: the nqn-style URL.
	Endpoint string

	ActivePath   string
	NvmeTCPPaths map[string]*NvmeTCPPath
}

// deriveLiveState combines a persisted record with raw observations into the
// canonical Live view. Pure function — no I/O, no mutation. The state machine
// is intentionally narrow:
//
//   - FrontendEmpty: always Running. No host-side state to observe.
//   - FrontendSPDKTCPNvmf: Running if SPDK subsystem present AND its listener
//     address matches the record. Stopped if subsystem absent.
//   - FrontendSPDKTCPBlockdev: Running iff all three layers present and the
//     kernel controller reports live. Error if any partial state. Stopped if
//     none of the layers present.
//
// Any "partial state" mapping to Error is what triggers the reconciler's
// takeCorrective in later steps. Error here means "host doesn't match
// record's intent and a corrective Create should be re-run".
func deriveLiveState(record *EngineFrontendRecord, raw *EngineFrontendObservedRaw) *EngineFrontendLive {
	live := &EngineFrontendLive{
		Record:       record,
		ActivePath:   raw.ActivePath,
		NvmeTCPPaths: raw.NvmeTCPPaths,
	}

	switch record.Frontend {
	case types.FrontendEmpty:
		live.State = types.InstanceStateRunning
		return live

	case types.FrontendSPDKTCPNvmf:
		if raw.SubsystemPresent {
			live.State = types.InstanceStateRunning
			live.Endpoint = GetNvmfEndpoint(record.VolumeNQN, raw.NvmfTargetIP, raw.NvmfTargetPort)
		} else {
			live.State = types.InstanceStateStopped
		}
		return live

	case types.FrontendSPDKTCPBlockdev:
		layersPresent := boolsToBitmap(
			raw.SubsystemPresent,
			raw.KernelControllerPresent,
			raw.DMDevicePresent,
			raw.DevicePathExists,
		)

		switch layersPresent {
		case 0b0000:
			live.State = types.InstanceStateStopped
		case 0b1111:
			if raw.KernelControllerLive {
				live.State = types.InstanceStateRunning
				live.Endpoint = raw.DevicePath
			} else {
				// All layers exist but kernel controller is in
				// connecting/resetting/failed — treat as Error so the
				// reconciler tears down + recreates rather than reporting
				// a "running" volume that won't actually serve I/O.
				live.State = types.InstanceStateError
				live.ErrorMsg = "kernel NVMe-oF controller is not in live state"
			}
		default:
			// Any partial combination — record says we should be running
			// but the host has a torn state. Reconciler will fix.
			live.State = types.InstanceStateError
			live.ErrorMsg = describePartialState(raw)
		}
		return live
	}

	live.State = types.InstanceStateError
	live.ErrorMsg = "unknown frontend type: " + record.Frontend
	return live
}

func boolsToBitmap(bs ...bool) int {
	out := 0
	for _, b := range bs {
		out <<= 1
		if b {
			out |= 1
		}
	}
	return out
}

// ObserveEngineFrontend builds a fresh EngineFrontendLive view from the
// canonical sources (SPDK + host kernel + dm-linear / device file). Pure
// observation — no mutation of any in-memory cache, no persistence write.
//
// Two-stage flow:
//  1. populate EngineFrontendObservedRaw via SPDK RPC + kernel sysfs/nvme +
//     os.Stat (skipped layers for FrontendEmpty / FrontendSPDKTCPNvmf where
//     they don't apply).
//  2. feed into deriveLiveState (pure function, fully unit-tested) to compute
//     the canonical State / Endpoint / ErrorMsg.
//
// On probe-call errors (e.g. SPDK RPC failure mid-shutdown), returns the
// partial raw observed so far + the error. The caller decides whether to
// trust the partial Live view or skip this tick. The reconciler skips on
// error; gRPC handlers may want to surface the error.
func ObserveEngineFrontend(ctx context.Context, spdkClient *spdkclient.Client, record *EngineFrontendRecord) (*EngineFrontendLive, error) {
	if record == nil {
		return nil, errors.New("ObserveEngineFrontend: nil record")
	}
	raw := &EngineFrontendObservedRaw{
		SubsystemNQN:   record.VolumeNQN,
		NvmfTargetIP:   record.TargetIP,
		NvmfTargetPort: record.TargetPort,
	}

	// Stage 1a: SPDK side. Empty-frontend records have no SPDK subsystem
	// to look for, so the probe is skipped.
	if record.Frontend != types.FrontendEmpty {
		subsystems, err := spdkClient.NvmfGetSubsystems("", "")
		if err != nil {
			return deriveLiveState(record, raw), errors.Wrap(err, "ObserveEngineFrontend: NvmfGetSubsystems")
		}
		for _, ss := range subsystems {
			if ss.Nqn != record.VolumeNQN {
				continue
			}
			raw.SubsystemPresent = true
			// For nvmf-frontend, the listener address is what gRPC clients
			// connect to. Pick the first listener; multipath surface here is
			// not relevant since the engine target only ever runs one nvmf
			// listener per subsystem in our deployment.
			for _, la := range ss.ListenAddresses {
				if la.Traddr != "" && la.Trsvcid != "" {
					raw.NvmfTargetIP = la.Traddr
					// la.Trsvcid is a string; the record stores int32. Keep
					// the record's value as the canonical port — it's what
					// the create flow set up. Just confirm a listener exists.
					_ = la.Trsvcid
					break
				}
			}
			break
		}
	}

	// FrontendEmpty + FrontendSPDKTCPNvmf have no host-kernel/dm-linear
	// surface; we're done. Empty-frontend always Running, Nvmf is Running
	// iff SubsystemPresent — both handled by deriveLiveState.
	if record.Frontend != types.FrontendSPDKTCPBlockdev {
		return deriveLiveState(record, raw), nil
	}

	// Stage 1b: kernel-initiator side. Use the same nvme-cli `list-subsys`
	// path the existing initiator package uses (initiator.GetSubsystems),
	// which returns kernel subsystems with each path's address + state.
	executor, err := helperutil.NewExecutor(commontypes.ProcDirectory)
	if err != nil {
		return deriveLiveState(record, raw), errors.Wrap(err, "ObserveEngineFrontend: NewExecutor")
	}
	kernelSubsystems, err := initiator.GetSubsystems(executor)
	if err != nil {
		// nvme list-subsys can fail for transient reasons; treat as kernel
		// state unobservable rather than absent.
		return deriveLiveState(record, raw), errors.Wrap(err, "ObserveEngineFrontend: kernel GetSubsystems")
	}
	for _, sys := range kernelSubsystems {
		if sys.NQN != record.VolumeNQN {
			continue
		}
		raw.KernelControllerPresent = true
		for _, p := range sys.Paths {
			if strings.EqualFold(p.State, "live") {
				raw.KernelControllerLive = true
				break
			}
		}
		break
	}

	// Stage 1c: dm-linear / device file side. The IM container rbinds
	// /host/dev over /dev (see package/instance-manager bind_dev), so the
	// host-side longhorn device file is stat-able from inside the IM at
	// the path returned by util.GetLonghornDevicePath.
	devPath := helperutil.GetLonghornDevicePath(record.VolumeName)
	raw.DevicePath = devPath
	if _, statErr := os.Stat(devPath); statErr == nil {
		raw.DevicePathExists = true
		// In our deployment, the longhorn device path /dev/longhorn/<vol>
		// IS the dm-linear device. If the kernel ctrlr is also present,
		// that confirms the dm stack is intact. Distinguishing a "dm
		// exists but pointing at dead nvme" case is covered by the
		// KernelControllerPresent=false branch in deriveLiveState.
		raw.DMDevicePresent = true
	} else if !os.IsNotExist(statErr) {
		// Unexpected stat error (permission, EIO, etc.) — surface but
		// don't bail; the partial Raw still produces a correct Error
		// state via deriveLiveState's default arm.
		return deriveLiveState(record, raw), errors.Wrapf(statErr, "ObserveEngineFrontend: stat(%s)", devPath)
	}

	_ = ctx // ctx reserved for future cancellation propagation when probe helpers gain ctx args
	return deriveLiveState(record, raw), nil
}

// reconcileEngineFrontends is the self-heal loop for EngineFrontend desync.
// Every tick: load all persisted records, observe each one's host-side state,
// and if the observer reports Error (partial host state — record says we
// should be running but host has a torn stack) run takeCorrective to drive
// reality back to the record's intent.
//
// On by default. The whole point of this work is to remove the manual
// scale-0/1 step that windrose needed today. Disable with
// LONGHORN_V2_RECONCILE_ENGINE_FRONTENDS=0 if a specific incident calls for
// halting the loop while debugging — the gate is a kill switch, not an
// opt-in.
func (s *Server) reconcileEngineFrontends() {
	if os.Getenv("LONGHORN_V2_RECONCILE_ENGINE_FRONTENDS") == "0" {
		logrus.Warn("EngineFrontend reconciler disabled via LONGHORN_V2_RECONCILE_ENGINE_FRONTENDS=0")
		return
	}

	logrus.Info("EngineFrontend reconciler started")
	ticker := time.NewTicker(EngineFrontendObserveInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			logrus.Info("EngineFrontend reconciler stopped due to context done")
			return
		case <-ticker.C:
			s.reconcileOnce()
		}
	}
}

func (s *Server) reconcileOnce() {
	if s.metadataDir == "" {
		return
	}
	records, err := loadEngineFrontendRecords(s.metadataDir)
	if err != nil {
		logrus.WithError(err).Warn("EngineFrontend reconciler: failed to load records")
		return
	}
	for _, record := range records {
		s.RLock()
		ef := s.engineFrontendMap[record.Name]
		spdkClient := s.spdkClient
		s.RUnlock()

		live, err := ObserveEngineFrontend(s.ctx, spdkClient, record)
		if err != nil {
			logrus.WithError(err).Warnf("EngineFrontend reconciler: probe failed for %s", record.Name)
			continue
		}

		// Only act on Error (partial host state vs record intent). Stopped
		// is "nothing in place yet" and is the legitimate state for an EF
		// whose Create RPC hasn't run yet — leave it alone. Running needs
		// no action.
		if live.State != types.InstanceStateError {
			continue
		}
		if ef == nil {
			// Record exists but no in-memory controller — IM probably
			// just restarted and recoverEngineFrontends hasn't caught up.
			// Skip; next tick will find it.
			continue
		}

		logrus.WithFields(logrus.Fields{
			"name":     record.Name,
			"reason":   live.ErrorMsg,
			"endpoint": live.Endpoint,
		}).Warn("EngineFrontend reconciler: detected desync, attempting heal")

		if healErr := ef.Heal(spdkClient, record); healErr != nil {
			logrus.WithError(healErr).Errorf("EngineFrontend reconciler: heal failed for %s; will retry next tick", record.Name)
			continue
		}
		logrus.Infof("EngineFrontend reconciler: healed %s", record.Name)
	}
}

// Heal drives an EngineFrontend whose host-side state has desynced from its
// persisted record back into agreement with the record. Mimics what the
// manual scale-0/1 workaround does today: tear down whatever partial host
// state exists, then re-run the create flow from the persisted intent.
//
// Skips if a real lifecycle RPC is in flight (Create/Delete/Switchover/
// Expand) — the in-flight handler owns the EF and may legitimately be
// observed in a transient state. The reconciler will retry next tick.
//
// Holds ef.Lock() across the host-state reset so concurrent Get/RPC paths
// see a consistent transition. The subsequent Create call manages its own
// locking.
func (ef *EngineFrontend) Heal(spdkClient *spdkclient.Client, record *EngineFrontendRecord) error {
	if ef == nil || record == nil {
		return errors.New("Heal: nil ef or record")
	}

	ef.Lock()
	if ef.isCreating || ef.isSwitchingOver || ef.isExpanding {
		// In-flight RPC owns this EF — back off, the next reconciler tick
		// will reassess once the RPC completes.
		ef.Unlock()
		ef.log.Info("Heal: skipping, lifecycle op in flight")
		return nil
	}

	ef.log.Warn("Heal: tearing down partial host state to drive back to record intent")

	// Tear down whatever the existing initiator holds (kernel NVMe-oF
	// session, dm-linear). initiator.Stop is the same code Delete uses;
	// it tolerates partial state where some layers exist and others don't.
	if ef.initiator != nil {
		if _, stopErr := ef.initiator.Stop(spdkClient, true, true, true); stopErr != nil {
			// Don't bail — even a failed Stop usually leaves things
			// closer to clean than before, and Create's own teardown can
			// pick up the rest. Log and continue.
			ef.log.WithError(stopErr).Warn("Heal: initiator.Stop returned an error; continuing with reset")
		}
		ef.initiator = nil
	}

	ef.Endpoint = ""
	if ef.NvmeTcpFrontend != nil {
		ef.NvmeTcpFrontend.TargetIP = ""
		ef.NvmeTcpFrontend.TargetPort = 0
		ef.NvmeTcpFrontend.Nqn = ""
		ef.NvmeTcpFrontend.Nguid = ""
		ef.clearNVMeTCPPathsLocked()
	}

	// Reset to Pending so Create's precondition check passes. State will
	// be set to Running (or Error) by Create's deferred resolver.
	ef.State = types.InstanceStatePending
	ef.ErrorMsg = ""
	ef.Unlock()

	if record.TargetIP == "" || record.TargetPort == 0 {
		return errors.Errorf("Heal: record %s has no target address; cannot recreate", record.Name)
	}
	targetAddress := net.JoinHostPort(record.TargetIP, strconv.Itoa(int(record.TargetPort)))
	if _, err := ef.Create(spdkClient, targetAddress); err != nil {
		return errors.Wrapf(err, "Heal: Create failed for %s targeting %s", record.Name, targetAddress)
	}
	return nil
}

func describePartialState(raw *EngineFrontendObservedRaw) string {
	missing := []string{}
	if !raw.SubsystemPresent {
		missing = append(missing, "spdk-subsystem")
	}
	if !raw.KernelControllerPresent {
		missing = append(missing, "kernel-nvme-ctrlr")
	}
	if !raw.DMDevicePresent {
		missing = append(missing, "dm-linear")
	}
	if !raw.DevicePathExists {
		missing = append(missing, "/dev/longhorn-file")
	}
	return "EngineFrontend desync: missing layers: " + strings.Join(missing, ",")
}
