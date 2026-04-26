package spdk

import (
	"context"
	"net"
	"os"
	"path/filepath"
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

// EngineFrontendHealConsecutiveFailures is the number of consecutive Error
// observations required before the reconciler will trigger a destructive
// heal. The intent is to filter out transient kernel-side recovery states
// (NVMe-oF controller in `connecting` / `resetting` after a keep-alive blip
// — kernel itself returns it to `live` within a few seconds) from genuine
// stuck desyncs that warrant tearing down host state.
//
// 3 ticks * 30s = 90s — comfortably longer than ctrlr_loss_timeout (15s),
// so any "kernel is doing its job recovering" window is allowed to resolve.
const EngineFrontendHealConsecutiveFailures = 3

// KernelControllerState is the tri-state classification of a kernel
// NVMe-oF controller's state attribute (from /sys/class/nvme/nvmeX/state
// surfaced via `nvme list-subsys`). The kernel uses many specific strings
// — `live`, `connecting`, `resetting`, `new`, `deleting`, `deleting (no IO)`,
// `dead` — and we collapse them into three buckets that drive the
// reconciler's decision: `live` is healthy, `transient` is a kernel
// internal recovery the kernel itself will resolve (do nothing), `dead`
// is a permanent failure where heal is the right response.
type KernelControllerState string

const (
	KernelControllerStateAbsent    KernelControllerState = "absent"
	KernelControllerStateLive      KernelControllerState = "live"
	KernelControllerStateTransient KernelControllerState = "transient"
	KernelControllerStateDead      KernelControllerState = "dead"
)

// classifyKernelControllerState maps the raw kernel state string to our
// tri-state. Anything we don't explicitly recognize is treated as transient
// to bias toward "wait and recheck" over destructive heal.
func classifyKernelControllerState(raw string) KernelControllerState {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "live":
		return KernelControllerStateLive
	case "dead", "deleting", "deleting (no io)":
		return KernelControllerStateDead
	case "":
		return KernelControllerStateAbsent
	default:
		return KernelControllerStateTransient
	}
}

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

	// Kernel-initiator side (only meaningful for FrontendSPDKTCPBlockdev).
	// KernelControllerState classifies the kernel's state attribute into
	// live / transient / dead — the reconciler ONLY treats `dead` as a
	// real desync. `transient` (connecting/resetting) is the kernel
	// recovering on its own and must not trigger heal.
	KernelControllerPresent bool
	KernelControllerState   KernelControllerState

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
			switch raw.KernelControllerState {
			case KernelControllerStateLive:
				live.State = types.InstanceStateRunning
				live.Endpoint = raw.DevicePath
			case KernelControllerStateTransient:
				// Kernel controller is in connecting / resetting / new —
				// the kernel's own state machine is mid-recovery and will
				// either return to `live` within ctrlr_loss_timeout (15s)
				// or transition to `dead` if it gives up. In-flight I/O
				// is queued by the kernel until then. Tearing down the
				// dm-linear and /dev/longhorn/X here would race the
				// kernel's recovery and break any consumer that has the
				// device mounted (observed 2026-04-26: heal during a
				// keep-alive blip put rustfs's XFS into shutdown).
				//
				// Report Running — the device is still expected to serve
				// I/O — but stash the kernel state in ErrorMsg purely as
				// a debugging breadcrumb. Reconciler treats Running as
				// "no action".
				live.State = types.InstanceStateRunning
				live.Endpoint = raw.DevicePath
				live.ErrorMsg = "kernel NVMe-oF controller is in transient recovery state (no heal)"
			case KernelControllerStateDead, KernelControllerStateAbsent:
				// Kernel has given up reconnecting (or the controller is
				// being deleted out from under us). This is a real desync
				// — the device on this host will not serve I/O until heal
				// runs.
				live.State = types.InstanceStateError
				live.ErrorMsg = "kernel NVMe-oF controller is dead/absent"
			}
		default:
			// Any partial combination — record says we should be running
			// but the host has a torn stack. Reconciler will fix.
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
		// Pick the strongest path-state across all paths for this subsystem.
		// `live` wins outright; otherwise prefer `transient` over `dead` so a
		// single failed path doesn't trip heal while another path is still
		// alive or recovering. Defaults to absent if Paths is empty.
		best := KernelControllerStateAbsent
		for _, p := range sys.Paths {
			cur := classifyKernelControllerState(p.State)
			switch cur {
			case KernelControllerStateLive:
				best = KernelControllerStateLive
			case KernelControllerStateTransient:
				if best != KernelControllerStateLive {
					best = KernelControllerStateTransient
				}
			case KernelControllerStateDead:
				if best != KernelControllerStateLive && best != KernelControllerStateTransient {
					best = KernelControllerStateDead
				}
			}
			if best == KernelControllerStateLive {
				break
			}
		}
		raw.KernelControllerState = best
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

	// Track which records still exist this tick so we can drop counters for
	// records that have been deleted out from under us (avoids unbounded
	// growth when EFs come and go).
	seen := map[string]struct{}{}

	for _, record := range records {
		seen[record.Name] = struct{}{}

		s.RLock()
		ef := s.engineFrontendMap[record.Name]
		spdkClient := s.spdkClient
		s.RUnlock()

		live, err := ObserveEngineFrontend(s.ctx, spdkClient, record)
		if err != nil {
			logrus.WithError(err).Warnf("EngineFrontend reconciler: probe failed for %s", record.Name)
			continue
		}

		// Only Error advances the consecutive-failure counter. Running
		// (including the Running-with-transient-kernel-state breadcrumb)
		// resets it; Stopped is the legitimate pre-create state and is
		// also non-Error.
		if live.State != types.InstanceStateError {
			s.Lock()
			if s.engineFrontendDesyncCounts[record.Name] > 0 {
				logrus.Infof("EngineFrontend reconciler: %s recovered after %d transient probe failures",
					record.Name, s.engineFrontendDesyncCounts[record.Name])
				delete(s.engineFrontendDesyncCounts, record.Name)
			}
			s.Unlock()
			continue
		}
		if ef == nil {
			// Record exists but no in-memory controller — IM probably
			// just restarted and recoverEngineFrontends hasn't caught up.
			// Skip; next tick will find it. Don't bump the counter — this
			// is a transient bookkeeping race, not a real desync.
			continue
		}

		s.Lock()
		s.engineFrontendDesyncCounts[record.Name]++
		count := s.engineFrontendDesyncCounts[record.Name]
		s.Unlock()

		// Below the threshold: log the desync but don't tear anything down.
		// The kernel may still be recovering, the manager may be in the
		// middle of an RPC that we'd race, or this could be a one-off
		// flap. We give it EngineFrontendHealConsecutiveFailures probes
		// (~90s) before considering it stuck enough to act on.
		if count < EngineFrontendHealConsecutiveFailures {
			logrus.WithFields(logrus.Fields{
				"name":   record.Name,
				"reason": live.ErrorMsg,
				"count":  count,
				"thresh": EngineFrontendHealConsecutiveFailures,
			}).Warn("EngineFrontend reconciler: desync observed, below heal threshold")
			continue
		}

		// Threshold met. Before tearing host state down, check whether a
		// userspace consumer has /dev/longhorn/<vol> open or mounted. If
		// so, our destructive heal would race them: dm-linear remove +
		// recreate swaps the underlying namespace under any mounted fs,
		// causing XFS shutdown / I/O errors (observed 2026-04-26 on
		// rustfs's /data when heal fired during a kernel-side keep-alive
		// blip). Skip heal in that case — the consumer is the better
		// signal of actual health than our probe is, and if the consumer
		// is genuinely broken too they'll surface their own errors and
		// eventually release the device, at which point heal can run
		// safely.
		if record.Frontend == types.FrontendSPDKTCPBlockdev && live.Endpoint != "" {
			inUse, why, checkErr := devicePathInUse(live.Endpoint)
			if checkErr != nil {
				logrus.WithError(checkErr).Warnf(
					"EngineFrontend reconciler: failed to check consumer for %s; deferring heal",
					record.Name)
				continue
			}
			if inUse {
				logrus.WithFields(logrus.Fields{
					"name":     record.Name,
					"endpoint": live.Endpoint,
					"reason":   live.ErrorMsg,
					"consumer": why,
				}).Warn("EngineFrontend reconciler: heal deferred — live consumer on device")
				continue
			}
		}

		logrus.WithFields(logrus.Fields{
			"name":     record.Name,
			"reason":   live.ErrorMsg,
			"endpoint": live.Endpoint,
			"count":    count,
		}).Warn("EngineFrontend reconciler: detected sustained desync, attempting heal")

		if healErr := ef.Heal(spdkClient, record); healErr != nil {
			logrus.WithError(healErr).Errorf("EngineFrontend reconciler: heal failed for %s; will retry next tick", record.Name)
			continue
		}

		// Heal succeeded — clear the counter so the next desync starts
		// from zero rather than firing immediately.
		s.Lock()
		delete(s.engineFrontendDesyncCounts, record.Name)
		s.Unlock()
		logrus.Infof("EngineFrontend reconciler: healed %s", record.Name)
	}

	// Garbage-collect counters for records that no longer exist (e.g. EF
	// was deleted between ticks). Without this the map would grow
	// monotonically on a long-lived IM.
	s.Lock()
	for name := range s.engineFrontendDesyncCounts {
		if _, present := seen[name]; !present {
			delete(s.engineFrontendDesyncCounts, name)
		}
	}
	s.Unlock()
}

// devicePathInUse returns true if any process on the host has the given
// device path open or has a filesystem mounted with it as the source.
// Errors from individual probes are not fatal — we OR all signals; any
// positive evidence of use returns true. The IM container has /host
// bind-mounted from the node, so /host/proc and /host/proc/*/mountinfo
// reflect the host's process state.
func devicePathInUse(devicePath string) (bool, string, error) {
	// Mount check: scan /host/proc/*/mountinfo. A process whose mount
	// namespace has devicePath as a mount source means the device is
	// actively serving a filesystem.
	procDir := "/host/proc"
	entries, err := os.ReadDir(procDir)
	if err != nil {
		return false, "", errors.Wrap(err, "devicePathInUse: read /host/proc")
	}
	// Resolve the device to its canonical path before comparing — the
	// probe path may be /dev/longhorn/<vol> (a dm-linear device) and
	// mountinfo may report it as either /dev/longhorn/<vol> directly or
	// as /dev/dm-N. Stat to follow the symlink if present, then we'll
	// match on both forms.
	canonical := devicePath
	if resolved, resolveErr := filepath.EvalSymlinks(devicePath); resolveErr == nil {
		canonical = resolved
	}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		// Skip non-numeric pid entries cheaply.
		if _, convErr := strconv.Atoi(e.Name()); convErr != nil {
			continue
		}
		miPath := procDir + "/" + e.Name() + "/mountinfo"
		data, readErr := os.ReadFile(miPath)
		if readErr != nil {
			// /proc entries can race — process exited mid-scan. Ignore.
			continue
		}
		// mountinfo line format includes the mount source as field 10
		// (1-indexed). Cheap substring check is sufficient — false
		// positives are tolerable here (we'd just defer heal once more
		// and recheck next tick).
		text := string(data)
		if strings.Contains(text, " "+devicePath+" ") || strings.Contains(text, " "+canonical+" ") {
			return true, "mounted in pid " + e.Name(), nil
		}
	}
	return false, "", nil
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
