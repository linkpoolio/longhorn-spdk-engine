package spdk

import (
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/sirupsen/logrus"

	"github.com/longhorn/go-spdk-helper/pkg/jsonrpc"
	spdkclient "github.com/longhorn/go-spdk-helper/pkg/spdk/client"
	spdktypes "github.com/longhorn/go-spdk-helper/pkg/spdk/types"
	helpertypes "github.com/longhorn/go-spdk-helper/pkg/types"

	safelog "github.com/longhorn/longhorn-spdk-engine/pkg/log"
	"github.com/longhorn/longhorn-spdk-engine/pkg/types"
)

// ReplicaReconcileInterval controls how often the reconciler scans persisted
// replica records and heals desync. 30s matches the EngineFrontend cadence.
const ReplicaReconcileInterval = 30 * time.Second

// ReplicaHealConsecutiveFailures is the number of consecutive
// exposed-but-no-listener observations required before the Replica reconciler
// will fire heal. Same rationale as EngineFrontendHealConsecutiveFailures:
// require a sustained desync before acting. SPDK probe errors (BdevGetBdevs /
// NvmfGetSubsystems hitting a busy reactor for a tick) do NOT count toward
// this threshold — a failed probe says nothing about the listener, and Heal
// would be just as likely to fail against the same busy target. Replica heal
// is much less destructive than EngineFrontend heal — there's no userspace fs
// mount to break, just brief I/O pauses on peer engines reconnecting — so
// this guard is mostly belt-and-braces, but the cost is negligible.
const ReplicaHealConsecutiveFailures = 3

// replicaHealSignal classifies a single reconciler observation of one replica.
type replicaHealSignal string

const (
	// replicaHealSignalHealthy: the replica should be exposed and SPDK shows
	// a listener for it.
	replicaHealSignalHealthy replicaHealSignal = "healthy"
	// replicaHealSignalCandidate: the in-memory replica says it should be
	// exposed (r.IsExposed) but SPDK shows no subsystem listener — the heal
	// target condition ("head lvol present, NVMe-oF listener missing").
	replicaHealSignalCandidate replicaHealSignal = "heal-candidate"
	// replicaHealSignalNone: nothing to heal — the replica isn't supposed to
	// be exposed (cleanly stopped or not yet created), or the head lvol is
	// gone (Heal cannot recover a missing head; the manager must drive a
	// fresh Create or accept replica failure).
	replicaHealSignalNone replicaHealSignal = "none"
	// replicaHealSignalSkipProbe: the SPDK probe itself failed, so the
	// observation is unusable; it must not count toward (or reset) the heal
	// threshold.
	replicaHealSignalSkipProbe replicaHealSignal = "skip-probe-error"
)

// deriveReplicaHealSignal classifies one observation. shouldBeExposed is the
// in-memory intent (Replica.IsExposed — the record has no exposure field);
// derived is the BuildReplicaFromRecord-style observation; probeErr is any
// SPDK probe failure for this tick. Pure function so the reconciler's heal
// condition is unit-testable.
func deriveReplicaHealSignal(shouldBeExposed bool, derived *Replica, probeErr error) replicaHealSignal {
	if probeErr != nil || derived == nil || derived.State == types.InstanceStateError {
		// Probe failures and structurally broken observations carry no
		// information about the listener; skip the tick entirely.
		return replicaHealSignalSkipProbe
	}
	if !shouldBeExposed {
		return replicaHealSignalNone
	}
	if derived.Head == nil {
		// Head lvol absent (derived Stopped): not the listener-missing case
		// and not recoverable by re-exposing.
		return replicaHealSignalNone
	}
	if derived.IsExposed {
		return replicaHealSignalHealthy
	}
	return replicaHealSignalCandidate
}

// Heal drives a Replica whose host-side state has desynced from its persisted
// record back into agreement. For replicas, the recoverable failure mode is
// "head lvol present but not exposed" — re-run the StartExposeBdev call from
// the record's IP / port to restore the listener.
//
// "Stopped" (head lvol absent) is non-recoverable here — the lvol either
// was never created or was destroyed externally. The owning controller
// (longhorn-manager) must drive a fresh Create or accept replica failure.
//
// Holds r.Lock() across the expose call so concurrent RPCs see a consistent
// view; skips when an in-flight rebuild / clone / restore is in progress.
func (r *Replica) Heal(spdkClient *spdkclient.Client, record *ReplicaRecord) error {
	if r == nil || record == nil {
		return errors.New("Replica.Heal: nil replica or record")
	}

	r.Lock()
	if r.isRebuilding || r.isSnapshotCloning || r.isRestoring {
		r.Unlock()
		r.log.Info("Replica.Heal: skipping, in-flight rebuild/clone/restore")
		return nil
	}
	if r.State == types.InstanceStateTerminating {
		r.Unlock()
		return nil
	}

	// Confirm the head lvol exists before we try to re-expose. If the head
	// is gone, Heal can't recover.
	alias := record.LvsName + "/" + record.Name
	if _, err := spdkClient.BdevLvolGetByName(alias, 0); err != nil {
		r.Unlock()
		if jsonrpc.IsJSONRPCRespErrorNoSuchDevice(err) {
			return errors.Errorf("Replica.Heal: head lvol %s absent; cannot recover (data loss territory)", alias)
		}
		return errors.Wrapf(err, "Replica.Heal: BdevLvolGetByName(%s)", alias)
	}

	r.log.Warn("Replica.Heal: re-exposing missing NVMe-oF listener from persisted record")

	params := r.healExposeParams(record)
	// Best-effort tear down any partial subsystem state first; ignore
	// not-found because the whole point is to recover from a missing
	// subsystem.
	if err := spdkClient.StopExposeBdev(params.Nqn); err != nil && !jsonrpc.IsJSONRPCRespErrorNoSuchDevice(err) {
		r.log.WithError(err).Warn("Replica.Heal: StopExposeBdev returned an error; continuing with re-expose")
	}

	port := strconv.Itoa(int(params.Port))
	if err := spdkClient.StartExposeBdevWithTransport(params.Nqn, alias, params.Nguid, params.IP, port, params.Transport.ToSPDKTransportType()); err != nil {
		r.State = types.InstanceStateError
		r.ErrorMsg = err.Error()
		r.Unlock()
		return errors.Wrapf(err, "Replica.Heal: StartExposeBdevWithTransport for %s", record.Name)
	}
	// Mirror Create's expose sequence: on RDMA nodes the replica also serves
	// a secondary TCP listener at primary+1 so TCP-only engines can attach.
	if params.NeedTCPFallback {
		if err := r.addTCPFallbackListener(spdkClient, params.Nqn, params.IP, params.Port); err != nil {
			r.State = types.InstanceStateError
			r.ErrorMsg = err.Error()
			r.Unlock()
			return errors.Wrapf(err, "Replica.Heal: addTCPFallbackListener for %s", record.Name)
		}
	}

	r.IsExposed = true
	r.State = types.InstanceStateRunning
	r.ErrorMsg = ""
	r.Unlock()

	r.log.Info("Replica.Heal: listener restored")
	select {
	case r.UpdateCh <- nil:
	default:
	}
	return nil
}

// replicaExposeParams are the NVMe-oF expose parameters Heal re-applies. They
// must mirror what Create used (same NQN, the same generated NGUID — Create
// passes generateNGUID(name) — the record's IP/primary port, the node
// transport, and the TCP fallback listener on RDMA nodes), otherwise a healed
// replica advertises a different namespace identity than the one engines
// originally attached.
type replicaExposeParams struct {
	Nqn             string
	Nguid           string
	IP              string
	Port            int32
	Transport       NvmfTransportType
	NeedTCPFallback bool
}

func (r *Replica) healExposeParams(record *ReplicaRecord) replicaExposeParams {
	return replicaExposeParams{
		Nqn:             helpertypes.GetNQN(record.Name),
		Nguid:           generateNGUID(record.Name),
		IP:              record.IP,
		Port:            record.PortStart,
		Transport:       r.transport(),
		NeedTCPFallback: r.transport().IsRDMA(),
	}
}

// reconcileReplicas is the self-heal loop for replica desync. Every tick:
// load all persisted replica records, observe each one's SPDK-side state
// (one shared bdev + subsystem dump per tick), and when the in-memory replica
// says it should be exposed but SPDK shows no NVMe-oF listener for it
// (deriveReplicaHealSignal == heal-candidate) for
// ReplicaHealConsecutiveFailures consecutive ticks, call Heal to re-run the
// expose sequence from the record. SPDK probe errors are skipped and never
// count toward the threshold.
//
// Complementary to s.monitoring()'s verify() loop: verify() reconciles the
// cached *Replica (workflow flags + per-operation state) against SPDK every
// 3s but does not heal listener desync. The cached map is no longer
// load-bearing for read paths — those go through BuildReplicaFromRecord
// directly — so the cache only serves write-side per-replica mutex
// serialisation now.
//
// On by default. LONGHORN_V2_RECONCILE_REPLICAS=0 disables for incident
// response only.
func (s *Server) reconcileReplicas() {
	if os.Getenv("LONGHORN_V2_RECONCILE_REPLICAS") == "0" {
		logrus.Warn("Replica reconciler disabled via LONGHORN_V2_RECONCILE_REPLICAS=0")
		return
	}

	logrus.Info("Replica reconciler started")
	ticker := time.NewTicker(ReplicaReconcileInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			logrus.Info("Replica reconciler stopped due to context done")
			return
		case <-ticker.C:
			s.reconcileReplicasOnce()
		}
	}
}

func (s *Server) reconcileReplicasOnce() {
	if s.metadataDir == "" {
		return
	}
	records, err := loadReplicaRecords(s.metadataDir)
	if err != nil {
		logrus.WithError(err).Warn("Replica reconciler: failed to load records")
		return
	}

	s.RLock()
	spdkClient := s.spdkClient
	nodeTransport := s.nodeTransport
	s.RUnlock()

	// One SPDK observation per tick, shared across all replicas (same dumps
	// ReplicaList uses) instead of a full unfiltered BdevGetBdevs per replica.
	bdevList, probeErr := spdkClient.BdevGetBdevs("", 0)
	var subsystems []spdktypes.NvmfSubsystem
	if probeErr == nil {
		subsystems, probeErr = spdkClient.NvmfGetSubsystems("", "")
		if probeErr != nil && jsonrpc.IsJSONRPCRespErrorNoSuchDevice(probeErr) {
			subsystems, probeErr = nil, nil
		}
	}
	if probeErr != nil {
		// A failed probe says nothing about listeners; do not count it
		// toward (or reset) any heal threshold — just retry next tick.
		logrus.WithError(probeErr).Warn("Replica reconciler: SPDK probe failed; skipping tick")
		return
	}

	// Track which records we observed so we can GC stale counters at the
	// end of the tick (records deleted between ticks).
	seen := map[string]struct{}{}

	for name, record := range records {
		seen[name] = struct{}{}

		s.RLock()
		r := s.replicaMap[name]
		s.RUnlock()

		if r == nil {
			// No cached *Replica yet — verify() hasn't reconciled the
			// cache after a recent IM restart, so there is no in-memory
			// exposure intent to compare against. Skip; next tick will
			// catch it. Don't bump the counter for this bookkeeping
			// window.
			continue
		}

		r.RLock()
		shouldBeExposed := r.IsExposed
		r.RUnlock()

		derived, deriveErr := buildReplicaFromObservation(record, nodeTransport, bdevList, subsystems)
		signal := deriveReplicaHealSignal(shouldBeExposed, derived, deriveErr)

		switch signal {
		case replicaHealSignalSkipProbe:
			// Unusable observation — leave the counter untouched so probe
			// blips neither accumulate toward heal nor mask a real desync.
			logrus.Warnf("Replica reconciler: unusable observation for %s; skipping (counter unchanged)", record.Name)
			continue
		case replicaHealSignalHealthy, replicaHealSignalNone:
			s.Lock()
			if s.replicaDesyncCounts[record.Name] > 0 {
				logrus.Infof("Replica reconciler: %s recovered after %d desync observations",
					record.Name, s.replicaDesyncCounts[record.Name])
				delete(s.replicaDesyncCounts, record.Name)
			}
			s.Unlock()
			continue
		case replicaHealSignalCandidate:
			// Fall through to the counter/heal logic below.
		}

		s.Lock()
		s.replicaDesyncCounts[record.Name]++
		count := s.replicaDesyncCounts[record.Name]
		s.Unlock()

		// Below threshold: log the desync and wait. Only a sustained
		// exposed-but-no-listener condition across multiple ticks justifies
		// firing the re-expose flow.
		if count < ReplicaHealConsecutiveFailures {
			logrus.WithFields(logrus.Fields{
				"name":   record.Name,
				"reason": "replica should be exposed but SPDK shows no NVMe-oF listener",
				"count":  count,
				"thresh": ReplicaHealConsecutiveFailures,
			}).Warn("Replica reconciler: desync observed, below heal threshold")
			continue
		}

		logrus.WithFields(logrus.Fields{
			"name":   record.Name,
			"reason": "replica should be exposed but SPDK shows no NVMe-oF listener",
			"count":  count,
		}).Warn("Replica reconciler: detected sustained desync, attempting heal")

		if healErr := r.Heal(spdkClient, record); healErr != nil {
			logrus.WithError(healErr).Errorf("Replica reconciler: heal failed for %s; will retry next tick", record.Name)
			continue
		}

		// Heal succeeded — reset counter so the next desync starts fresh.
		s.Lock()
		delete(s.replicaDesyncCounts, record.Name)
		s.Unlock()
		logrus.Infof("Replica reconciler: healed %s", record.Name)
	}

	// GC counters for records that no longer exist (e.g. replica deleted
	// between ticks). Without this the map would grow monotonically on a
	// long-lived IM with replicas churning.
	s.Lock()
	for name := range s.replicaDesyncCounts {
		if _, present := seen[name]; !present {
			delete(s.replicaDesyncCounts, name)
		}
	}
	s.Unlock()
}

// BuildReplicaFromRecord constructs a transient *Replica populated from a
// fresh SPDK observation plus the persisted record. Used by ReplicaGet /
// ReplicaList / ReplicaWatch to serve reads without consulting
// s.replicaMap, so the cache is not load-bearing for read paths.
//
// The returned struct is suitable for ServiceReplicaToProtoReplica. It
// does not carry workflow state (isRebuilding, isSnapshotCloning, etc.);
// those stay on the cached *Replica owned by mutating handlers. The
// cache continues to provide per-replica mutex serialisation for writes.
//
// On any SPDK probe failure, returns a *Replica with State=InstanceStateError
// and an explanatory ErrorMsg so the gRPC client gets a coherent answer
// instead of a server error.
func BuildReplicaFromRecord(spdkClient *spdkclient.Client, record *ReplicaRecord, nodeTransport NvmfTransportType) (*Replica, error) {
	if record == nil {
		return nil, errors.New("BuildReplicaFromRecord: nil record")
	}

	// Read all bdevs plus the nvmf subsystem list once; the actual derivation
	// is the pure buildReplicaFromObservation, which batch callers
	// (ReplicaList, the replica reconciler) feed with a single shared dump
	// instead of one full BdevGetBdevs per replica.
	bdevList, err := spdkClient.BdevGetBdevs("", 0)
	if err != nil {
		r := newReplicaSkeletonFromRecord(record, nodeTransport)
		r.State = types.InstanceStateError
		r.ErrorMsg = "BuildReplicaFromRecord: BdevGetBdevs: " + err.Error()
		return r, nil
	}
	subsystems, err := spdkClient.NvmfGetSubsystems("", "")
	if err != nil && !jsonrpc.IsJSONRPCRespErrorNoSuchDevice(err) {
		r := newReplicaSkeletonFromRecord(record, nodeTransport)
		r.State = types.InstanceStateError
		r.ErrorMsg = "BuildReplicaFromRecord: NvmfGetSubsystems: " + err.Error()
		return r, nil
	}

	return buildReplicaFromObservation(record, nodeTransport, bdevList, subsystems)
}

// newReplicaSkeletonFromRecord builds the identity-only *Replica that all
// derive paths start from. The logger is real (fix for a nil-deref panic:
// ServiceReplicaToProtoReplica logs through r.log when the BackingImage name
// fails to parse, which is reachable from the ReplicaGet/List gRPC read path).
func newReplicaSkeletonFromRecord(record *ReplicaRecord, nodeTransport NvmfTransportType) *Replica {
	log := logrus.StandardLogger().WithFields(logrus.Fields{
		"replicaName": record.Name,
		"lvsName":     record.LvsName,
		"lvsUUID":     record.LvsUUID,
		"derived":     true,
	})
	return &Replica{
		Name:              record.Name,
		Alias:             record.LvsName + "/" + record.Name,
		LvsName:           record.LvsName,
		LvsUUID:           record.LvsUUID,
		Nqn:               helpertypes.GetNQN(record.Name),
		IP:                record.IP,
		PortStart:         record.PortStart,
		PortEnd:           record.PortEnd,
		ListenerTransport: nodeTransport,
		ActiveChain:       []*Lvol{nil},
		SnapshotLvolMap:   map[string]*Lvol{},
		log:               safelog.NewSafeLogger(log),
	}
}

// buildReplicaFromObservation derives a transient *Replica from a persisted
// record plus prefetched SPDK dumps (all bdevs + all nvmf subsystems). Pure —
// no SPDK calls — so batch callers can share one dump across many replicas.
func buildReplicaFromObservation(record *ReplicaRecord, nodeTransport NvmfTransportType, bdevList []spdktypes.BdevInfo, subsystems []spdktypes.NvmfSubsystem) (*Replica, error) {
	if record == nil {
		return nil, errors.New("buildReplicaFromObservation: nil record")
	}

	r := newReplicaSkeletonFromRecord(record, nodeTransport)

	// constructSnapshotLvolMap and constructActiveChainFromSnapshotLvolMap
	// are pure functions that derive the snapshot tree from this map.
	bdevLvolMap := map[string]*spdktypes.BdevInfo{}
	for i := range bdevList {
		bdev := &bdevList[i]
		if !isReplicaBdev(bdev, record.Name, record.LvsUUID) {
			continue
		}
		bdevLvolMap[lvolBaseName(bdev)] = bdev
	}

	headBdev, headPresent := bdevLvolMap[record.Name]
	if !headPresent {
		// No head lvol on disk — replica is Stopped (created? destroyed?
		// either way nothing for ReplicaGet to render beyond identity).
		r.State = types.InstanceStateStopped
		return r, nil
	}

	// Derive snapshot tree.
	snapshotLvolMap, err := constructSnapshotLvolMap(record.Name, bdevLvolMap)
	if err != nil {
		r.State = types.InstanceStateError
		r.ErrorMsg = "BuildReplicaFromRecord: constructSnapshotLvolMap: " + err.Error()
		return r, nil
	}
	chain, err := constructActiveChainFromSnapshotLvolMap(record.Name, snapshotLvolMap, bdevLvolMap)
	if err != nil {
		r.State = types.InstanceStateError
		r.ErrorMsg = "BuildReplicaFromRecord: constructActiveChainFromSnapshotLvolMap: " + err.Error()
		return r, nil
	}
	if len(chain) == 0 {
		r.State = types.InstanceStateError
		r.ErrorMsg = "BuildReplicaFromRecord: empty active chain"
		return r, nil
	}
	r.Head = chain[len(chain)-1]
	r.ActiveChain = chain
	r.SnapshotLvolMap = snapshotLvolMap
	r.BackingImage = chain[0]
	if headBdev.BlockSize > 0 {
		r.SpecSize = headBdev.NumBlocks * uint64(headBdev.BlockSize)
	}
	// ActualSize aggregates head + all snapshots, matching the existing
	// construct() flow at replica.go:670-676. The chain's per-Lvol
	// ActualSize is populated by BdevLvolInfoToServiceLvol against
	// defaultClusterSize.
	if r.Head != nil {
		actual := r.Head.ActualSize
		for _, sl := range snapshotLvolMap {
			actual += sl.ActualSize
		}
		r.ActualSize = actual
	}

	// Derive expose state from the prefetched nvmf subsystem dump.
	nqn := r.Nqn
	for i := range subsystems {
		ss := &subsystems[i]
		if ss.Nqn != nqn {
			continue
		}
		if len(ss.ListenAddresses) > 0 {
			r.IsExposed = true
		}
		break
	}

	// BuildReplicaFromRecord runs only when the IM has no in-memory record for
	// this replica (post-restart, post-stop, or pre-create). In that path,
	// "head lvol present + no NVMe-oF listener" is the legitimate Stopped
	// state — the replica was previously cleanly stopped (record was kept on
	// disk because cleanupRequired=false) and the subsystem was correctly
	// deleted at stop. Marking Error here would loop the manager in
	// "all replicas failed → salvageRequested" forever because the salvage
	// flow gates on Status.CurrentState=Stopped, blocking detach.
	//
	// Desync detection for actively-running replicas (IM has in-memory record
	// but listener got dropped) is the responsibility of the periodic
	// reconciler driven by the in-memory map, not this derive-from-record
	// function.
	if r.IsExposed {
		r.State = types.InstanceStateRunning
	} else {
		r.State = types.InstanceStateStopped
	}
	return r, nil
}

// isReplicaBdev returns true if the bdev belongs to this replica's snapshot
// tree on this lvstore — head lvol, snapshot lvols, or backing image. Names
// of replica lvols all start with the replica name (head is exact, snapshots
// are <name>-snap-<id>).
func isReplicaBdev(bdev *spdktypes.BdevInfo, replicaName, lvsUUID string) bool {
	if bdev == nil || bdev.DriverSpecific == nil || bdev.DriverSpecific.Lvol == nil {
		return false
	}
	if len(bdev.Aliases) == 0 {
		return false
	}
	if bdev.DriverSpecific.Lvol.LvolStoreUUID != lvsUUID {
		return false
	}
	base := lvolBaseName(bdev)
	if base == replicaName {
		return true
	}
	return strings.HasPrefix(base, replicaName+"-")
}

// lvolBaseName returns the lvol name (without lvstore prefix) from a bdev
// alias of the form "<lvs>/<lvol>".
func lvolBaseName(bdev *spdktypes.BdevInfo) string {
	if bdev == nil || len(bdev.Aliases) == 0 {
		return ""
	}
	alias := bdev.Aliases[0]
	if i := strings.Index(alias, "/"); i >= 0 {
		return alias[i+1:]
	}
	return alias
}
