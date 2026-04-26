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

	"github.com/longhorn/longhorn-spdk-engine/pkg/types"
)

// ReplicaReconcileInterval controls how often the reconciler scans persisted
// replica records and heals desync. 30s matches the EngineFrontend cadence.
const ReplicaReconcileInterval = 30 * time.Second

// ReplicaHealConsecutiveFailures is the number of consecutive Error
// observations required before the Replica reconciler will fire heal.
// Same rationale as EngineFrontendHealConsecutiveFailures: filter out
// transient SPDK probe blips (BdevGetBdevs / NvmfGetSubsystems hitting a
// busy reactor for a tick) from genuine listener-missing desync. Replica
// heal is much less destructive than EngineFrontend heal — there's no
// userspace fs mount to break, just brief I/O pauses on peer engines
// reconnecting — so this guard is mostly belt-and-braces, but the cost is
// negligible.
const ReplicaHealConsecutiveFailures = 3

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

	nqn := helpertypes.GetNQN(record.Name)
	// Best-effort tear down any partial subsystem state first; ignore
	// not-found because the whole point is to recover from a missing
	// subsystem.
	if err := spdkClient.StopExposeBdev(nqn); err != nil && !jsonrpc.IsJSONRPCRespErrorNoSuchDevice(err) {
		r.log.WithError(err).Warn("Replica.Heal: StopExposeBdev returned an error; continuing with re-expose")
	}

	port := strconv.Itoa(int(record.PortStart))
	transport := r.transport().ToSPDKTransportType()
	if err := spdkClient.StartExposeBdevWithTransport(nqn, alias, "", record.IP, port, transport); err != nil {
		r.State = types.InstanceStateError
		r.ErrorMsg = err.Error()
		r.Unlock()
		return errors.Wrapf(err, "Replica.Heal: StartExposeBdevWithTransport for %s", record.Name)
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

// reconcileReplicas is the self-heal loop for replica desync. Every tick:
// load all persisted replica records, observe each one's SPDK-side state via
// BuildReplicaFromRecord, and when state == Error (head lvol on disk but no
// NVMe-oF listener exposed) call Heal to re-run StartExposeBdev from the
// record.
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

	// Track which records we observed so we can GC stale counters at the
	// end of the tick (records deleted between ticks).
	seen := map[string]struct{}{}

	for name, record := range records {
		seen[name] = struct{}{}

		s.RLock()
		r := s.replicaMap[name]
		spdkClient := s.spdkClient
		nodeTransport := s.nodeTransport
		s.RUnlock()

		// Single observation function — same one ReplicaGet/List use.
		// A *Replica back with State=Error means "head lvol on disk but
		// no listener exposed AND something probe-side is wrong" →
		// candidate for heal. Other states (Running, Stopped) require no
		// action from the reconciler.
		derived, err := BuildReplicaFromRecord(spdkClient, record, nodeTransport)
		if err != nil {
			logrus.WithError(err).Warnf("Replica reconciler: probe failed for %s", record.Name)
			continue
		}

		// Non-Error states reset the counter. Stopped is "the replica is
		// legitimately not exposed right now" — usually fine, the manager
		// will drive Create when it wants this exposed; not our job to
		// heal toward Running.
		if derived.State != types.InstanceStateError {
			s.Lock()
			if s.replicaDesyncCounts[record.Name] > 0 {
				logrus.Infof("Replica reconciler: %s recovered after %d transient probe failures",
					record.Name, s.replicaDesyncCounts[record.Name])
				delete(s.replicaDesyncCounts, record.Name)
			}
			s.Unlock()
			continue
		}
		if r == nil {
			// No cached *Replica yet — verify() hasn't reconciled the
			// cache after a recent IM restart. Skip; next tick will
			// catch it. Don't bump the counter for this bookkeeping
			// window.
			continue
		}

		s.Lock()
		s.replicaDesyncCounts[record.Name]++
		count := s.replicaDesyncCounts[record.Name]
		s.Unlock()

		// Below threshold: log the desync and wait. Most "Error" probe
		// outcomes for replicas are transient SPDK busy windows; only a
		// sustained Error across multiple ticks justifies firing the
		// re-expose flow.
		if count < ReplicaHealConsecutiveFailures {
			logrus.WithFields(logrus.Fields{
				"name":   record.Name,
				"reason": derived.ErrorMsg,
				"count":  count,
				"thresh": ReplicaHealConsecutiveFailures,
			}).Warn("Replica reconciler: desync observed, below heal threshold")
			continue
		}

		logrus.WithFields(logrus.Fields{
			"name":   record.Name,
			"reason": derived.ErrorMsg,
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

	log := logrus.StandardLogger().WithFields(logrus.Fields{
		"replicaName": record.Name,
		"lvsName":     record.LvsName,
		"lvsUUID":     record.LvsUUID,
		"derived":     true,
	})
	r := &Replica{
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
		log:               nil, // not used by ServiceReplicaToProtoReplica unless BackingImage extract fails
	}
	_ = log // log reserved for future error-path logging

	// Read all bdevs in this lvstore. constructSnapshotLvolMap and
	// constructActiveChainFromSnapshotLvolMap are pure functions that
	// derive the snapshot tree from this map.
	bdevList, err := spdkClient.BdevGetBdevs("", 0)
	if err != nil {
		r.State = types.InstanceStateError
		r.ErrorMsg = "BuildReplicaFromRecord: BdevGetBdevs: " + err.Error()
		return r, nil
	}
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

	// Derive expose state from nvmf subsystem.
	nqn := r.Nqn
	subsystems, err := spdkClient.NvmfGetSubsystems(nqn, "")
	if err != nil && !jsonrpc.IsJSONRPCRespErrorNoSuchDevice(err) {
		r.State = types.InstanceStateError
		r.ErrorMsg = "BuildReplicaFromRecord: NvmfGetSubsystems: " + err.Error()
		return r, nil
	}
	for _, ss := range subsystems {
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
