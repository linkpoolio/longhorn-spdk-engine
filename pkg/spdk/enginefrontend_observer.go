package spdk

import (
	"context"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"syscall"
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

// EngineFrontendObservedRaw holds primitive booleans + identity values
// gathered from SPDK and the host kernel by ObserveEngineFrontend, fed
// into deriveLiveState to compute the canonical Live view. Trivial to
// construct in tests so deriveLiveState can be exercised without mocking
// the entire SPDK + sysfs stack.
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

	// FrontendSPDKTCPNvmf side: the listener address. State for an Nvmf
	// frontend is purely whether the SPDK subsystem listener is up — no
	// local initiator or dm device.
	NvmfTargetIP   string
	NvmfTargetPort int32
}

// EngineFrontendLive is the derived runtime view of an EngineFrontend at
// one point in time, computed from the persisted record + raw observation
// by deriveLiveState. Throw-away — built on demand by gRPC handlers and
// the reconciler, never stored as authoritative state.
type EngineFrontendLive struct {
	Record *EngineFrontendRecord

	State    types.InstanceState
	ErrorMsg string

	// Endpoint is what gRPC clients see. For blockdev: /dev/longhorn/<vol>.
	// For nvmf: the nqn-style URL.
	Endpoint string
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
		Record: record,
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
				// Kernel controller is in connecting / resetting / new — the
				// kernel state machine is mid-recovery and will either return
				// to `live` within ctrlr_loss_timeout or transition to `dead`
				// if it gives up. In-flight I/O is queued by the kernel until
				// then. Tearing down dm-linear and /dev/longhorn/X here would
				// race the kernel's recovery and corrupt any filesystem
				// mounted on the device. Report Running and stash the
				// transient state in ErrorMsg as a debugging breadcrumb only.
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

// dmLinearIsLive reports whether the longhorn device file at devPath backs a
// live dm-linear mapping. os.Stat success is not enough: after `dmsetup remove`
// the mknod inode can survive (or not yet be re-cleaned by the IM) while the
// mapping is gone, and stat keeps succeeding on the orphaned file. The kernel
// only admits the table is gone when the device is opened — a torn-down
// dm-linear returns ENXIO. A non-blocking open is the cheapest race-free probe
// and matches what any real I/O consumer would observe. Returns false (rather
// than a stale-positive) for a dead device so deriveLiveState derives Error and
// reconcileOnce can heal it. Guards on ModeDevice first so a stray regular file
// at the path is never mistaken for a live mapping.
func dmLinearIsLive(devPath string) bool {
	statInfo, err := os.Stat(devPath)
	if err != nil || statInfo.Mode()&os.ModeDevice == 0 {
		return false
	}
	f, err := os.OpenFile(devPath, os.O_RDONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		return false
	}
	_ = f.Close()
	return true
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
func ObserveEngineFrontend(ctx context.Context, spdkClient *spdkclient.Client, record *EngineFrontendRecord) (live *EngineFrontendLive, err error) {
	if record == nil {
		return nil, errors.New("ObserveEngineFrontend: nil record")
	}
	raw := &EngineFrontendObservedRaw{
		NvmfTargetIP:   record.TargetIP,
		NvmfTargetPort: record.TargetPort,
	}

	// Diagnostic breadcrumb for the derived-state migration. Logged at Debug
	// so it is silent at the default (Notice/Info) log level and can be
	// turned on per-incident by raising data-engine-log-level. This is the
	// observability gap that hid the EngineFrontend status-sync regression:
	// a blockdev whose layers all probe absent derives Stopped (0b0000),
	// which reconcileOnce treats as non-Error and silently skips — so a
	// frontend that is physically up but observed-down left no trace. The
	// named return values let this fire on every exit path, capturing the
	// exact layer bitmap the observer saw alongside the state it derived.
	defer func() {
		logFields := logrus.Fields{
			"name":                    record.Name,
			"volumeName":              record.VolumeName,
			"frontend":                record.Frontend,
			"volumeNQN":               record.VolumeNQN,
			"subsystemPresent":        raw.SubsystemPresent,
			"kernelControllerPresent": raw.KernelControllerPresent,
			"kernelControllerState":   raw.KernelControllerState,
			"dmDevicePresent":         raw.DMDevicePresent,
			"devicePathExists":        raw.DevicePathExists,
			"devicePath":              raw.DevicePath,
			"nvmfTargetIP":            raw.NvmfTargetIP,
			"nvmfTargetPort":          raw.NvmfTargetPort,
		}
		if live != nil {
			logFields["derivedState"] = live.State
			logFields["derivedEndpoint"] = live.Endpoint
			if live.ErrorMsg != "" {
				logFields["derivedErrorMsg"] = live.ErrorMsg
			}
		}
		if err != nil {
			logFields["probeErr"] = err.Error()
		}
		logrus.WithFields(logFields).Debug("EngineFrontend observer: observed raw + derived state")
	}()

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
		raw.DMDevicePresent = dmLinearIsLive(devPath)
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
// Every tick: load all persisted records, observe each one's host-side
// state, and if the observer reports Error (partial host state — record
// says we should be running but host has a torn stack) drive reality
// back to the record's intent. Removes the operator-driven manual
// recovery cycle (scale workload to 0, wait for detach, scale back to 1)
// that was previously the only way to repair a stale host-side stack
// after IM crash recovery. LONGHORN_V2_RECONCILE_ENGINE_FRONTENDS=0
// disables the loop for emergency operator intervention.

// correctStaleEngineFrontendRecordTarget validates a persisted engine
// frontend record's target address against the locally running engine for
// the same volume. After an IM restart the engine is recreated on a fresh
// port while the frontend record still holds the old one; recovering or
// healing from that record dials a dead (or foreign) listener forever and
// the volume cycles through fault/salvage without converging. When the
// local engine's live target differs, the record is corrected in place so
// recovery and heal dial the real listener. Records whose engine is remote
// or not running locally are left untouched. Returns true when a
// correction was applied.
func (s *Server) correctStaleEngineFrontendRecordTarget(record *EngineFrontendRecord) bool {
	if record == nil || record.EngineName == "" || record.TargetIP == "" || record.TargetPort == 0 {
		return false
	}

	s.RLock()
	e := s.engineMap[record.EngineName]
	s.RUnlock()
	if e == nil {
		return false
	}

	eng := e.Get()
	if eng == nil || eng.State != string(types.InstanceStateRunning) || eng.Ip == "" || eng.Port == 0 {
		return false
	}
	if record.TargetIP == eng.Ip && record.TargetPort == eng.Port {
		return false
	}

	logrus.Warnf("Correcting stale engine frontend record %s: target %s:%d -> live engine target %s:%d",
		record.Name, record.TargetIP, record.TargetPort, eng.Ip, eng.Port)
	oldIP, oldPort := record.TargetIP, record.TargetPort
	record.TargetIP = eng.Ip
	record.TargetPort = eng.Port
	for _, path := range record.Paths {
		if path == nil {
			continue
		}
		// Only rewrite the path that mirrored the stale primary target; a
		// switchover secondary path pointing at another engine is not ours
		// to correct.
		if path.TargetIP == oldIP && path.TargetPort == oldPort {
			path.TargetIP = eng.Ip
			path.TargetPort = eng.Port
		}
	}
	return true
}

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

// bumpDesyncCountLocked increments the consecutive-Error-probe counter for the
// given EngineFrontend and returns the new count. The caller MUST hold s.Lock
// (reconcileOnce calls it under s.Lock). Extracted from reconcileOnce so the
// counter state machine is unit-testable without device probing.
func (s *Server) bumpDesyncCountLocked(name string) int {
	s.engineFrontendDesyncCounts[name]++
	return s.engineFrontendDesyncCounts[name]
}

// clearDesyncCountLocked resets the consecutive-Error-probe counter for the
// given EngineFrontend (called when a probe returns a non-Error state, i.e. the
// EF recovered). The caller MUST hold s.Lock. Logs the recovery if a non-zero
// counter was cleared.
func (s *Server) clearDesyncCountLocked(name string) {
	if s.engineFrontendDesyncCounts[name] > 0 {
		logrus.Infof("EngineFrontend reconciler: %s recovered after %d transient probe failures",
			name, s.engineFrontendDesyncCounts[name])
		delete(s.engineFrontendDesyncCounts, name)
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

		if s.correctStaleEngineFrontendRecordTarget(record) && ef != nil {
			ef.Lock()
			if ef.NvmeTcpFrontend != nil {
				ef.NvmeTcpFrontend.TargetIP = record.TargetIP
				ef.NvmeTcpFrontend.TargetPort = record.TargetPort
			}
			ef.Unlock()
			if err := saveEngineFrontendRecord(s.metadataDir, ef); err != nil {
				logrus.WithError(err).Warnf("EngineFrontend reconciler: failed to persist corrected record for %s", record.Name)
			}
		}

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
			s.clearDesyncCountLocked(record.Name)
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
		count := s.bumpDesyncCountLocked(record.Name)
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

		// Threshold met. Heal recreates dm-linear and the device file, swapping
		// the underlying NVMe namespace; a filesystem mounted on a LIVE device
		// would see its block device disappear and go into shutdown — so we must
		// NOT heal a volume that is genuinely serving I/O (a sustained false
		// Error). The discriminator is the device's ACTUAL health right now, NOT
		// whether it is mounted.
		//
		// The previous guard (devicePathInUse, a substring scan of
		// /host/proc/*/mountinfo) was presence-based and deferred heal FOREVER in
		// the exact case we need to fix: after an IM rollout the EngineFrontend's
		// kernel NVMe controller is gone and /dev/longhorn/<vol> is dead (EIO),
		// but the consumer's STALE globalmount is still mounted, so the scan
		// reported "live consumer on device" and skipped heal indefinitely (the
		// mode-J EF-stale-positive — observed downing ~20 volumes after the
		// .101/.102 rolls). A stale EIO mount is mounted but doing no successful
		// I/O, so mount-presence is the wrong signal.
		//
		// Re-probe the device instead (conservative open + 1-byte read): if it is
		// confirmed live (reads succeed), the Error was transient / it recovered,
		// so defer — do not heal a live volume out from under its consumer. If it
		// is dead or absent (heal is only reached after 3 sustained dead probes
		// anyway), any mount on it is already a stale, fs-shutdown mount, so
		// recreating the device corrupts nothing and IS the recovery — proceed.
		if record.Frontend == types.FrontendSPDKTCPBlockdev {
			devicePath := helperutil.GetLonghornDevicePath(record.VolumeName)
			if deviceReadsLive(devicePath) {
				logrus.WithFields(logrus.Fields{
					"name":       record.Name,
					"devicePath": devicePath,
					"reason":     live.ErrorMsg,
				}).Warn("EngineFrontend reconciler: heal deferred — device reads live now (transient Error / recovered); not healing a live volume")
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

// deviceReadsLive reports whether devPath is a present block device that can be
// opened non-blocking AND returns data (or a clean EOF) on a 1-byte read — i.e.
// it is genuinely serving I/O right now. A missing path, a non-device file, an
// ENXIO/EIO (or any) open failure, or an EIO/ENXIO read all report false. It is
// the inverse discriminator the heal guard needs: heal proceeds UNLESS the device
// is confirmed live, so a stale EIO mount on a dead/absent backing (the mode-J
// EF-stale-positive) no longer blocks heal, while a genuinely live volume (real
// reads succeed) is never healed out from under its consumer. Mirrors the
// conservative probe in suspendDeviceConfirmedDead, but as a positive
// liveness check that also treats an absent device as not-live.
func deviceReadsLive(devPath string) bool {
	statInfo, err := os.Stat(devPath)
	if err != nil || statInfo.Mode()&os.ModeDevice == 0 {
		return false
	}
	f, err := os.OpenFile(devPath, os.O_RDONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		return false
	}
	defer func() { _ = f.Close() }()
	buf := make([]byte, 1)
	if _, readErr := f.Read(buf); readErr != nil && !errors.Is(readErr, io.EOF) {
		return false
	}
	return true
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
	// Deletion guard: Delete closes stopCh and marks the EF Terminating; a
	// heal racing (or trailing) a delete would resurrect host state for a
	// frontend the manager is tearing down.
	deleting := false
	select {
	case <-ef.stopCh:
		deleting = true
	default:
	}
	if ef.isCreating || ef.isSwitchingOver || ef.isExpanding || deleting || ef.State == types.InstanceStateTerminating {
		// In-flight RPC owns this EF — back off, the next reconciler tick
		// will reassess once the RPC completes.
		ef.Unlock()
		ef.log.Info("Heal: skipping, lifecycle op in flight or frontend is being deleted")
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
