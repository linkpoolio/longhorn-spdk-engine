package spdk

import (
	"encoding/hex"
	"fmt"
	"net"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/google/uuid"

	"github.com/longhorn/types/pkg/generated/spdkrpc"

	spdkclient "github.com/longhorn/go-spdk-helper/pkg/spdk/client"
	spdktypes "github.com/longhorn/go-spdk-helper/pkg/spdk/types"

	"github.com/longhorn/longhorn-spdk-engine/pkg/client"
	"github.com/longhorn/longhorn-spdk-engine/pkg/types"
	"github.com/longhorn/longhorn-spdk-engine/pkg/util"
)

const (
	DiskTypeFilesystem = "filesystem"
	DiskTypeBlock      = "block"

	ReplicaRebuildingLvolSuffix  = "rebuilding"
	ReplicaExpiredLvolSuffix     = "expired"
	ReplicaCloningLvolSuffix     = "cloning"
	RebuildingSnapshotNamePrefix = "rebuild"

	SyncTimeout = 60 * time.Minute

	maxRetries    = 30
	retryInterval = 1 * time.Second

	disconnectMaxRetries    = 5
	disconnectRetryInterval = 1 * time.Second

	MaxShallowCopyWaitTime   = 72 * time.Hour
	ShallowCopyCheckInterval = 3 * time.Second

	MaxSnapshotCloneWaitTime         = 72 * time.Hour
	SnapshotCloneStatusCheckInterval = 3 * time.Second
)

const (
	replicaMultipath = "disable"
)

// Replica NVMe-oF timeouts. Defaults match upstream's const values but can be
// overridden per-IM via env vars set by longhorn-manager's instance_manager_controller
// from the data-engine Setting CRs. Kept as vars so all existing bare-identifier
// call sites continue to compile.
//
// replicaCtrlrLossTimeoutSec lowered from upstream 15s to 3s: when a remote
// replica IM disappears mid-rebuild, every RDMA_CM_EVENT_REJECTED from the
// dying peer triggers bdev_nvme_failover_ctrlr reactively (no cooldown),
// starving the local reactor until ctrlr_loss fires and the ctrlr is reaped.
// At 15s we saw spdk_tgt crash with broken-pipe on /var/tmp/spdk.sock. 3s
// trims the spam window below the liveness threshold.
//
// rebuildCtrlrLossTimeoutSec / rebuildFastIOFailTimeoutSec apply only to the
// three rebuild-path bdev_nvme attachments in replica.go (clone src->dst,
// rebuild src->dst-rebuilding-lvol, rebuild dst->src-snapshot). Rebuild is
// inherently restartable, so sub-second failover is safe here and makes
// teardown-during-rebuild crash-proof.
var (
	replicaCtrlrLossTimeoutSec  = 3
	replicaReconnectDelaySec    = 2
	replicaFastIOFailTimeoutSec = 10
	replicaTransportAckTimeout  = 10
	replicaKeepAliveTimeoutMs   = 10000
	// replicaTransportTos tags outbound NVMe-oF packets with DSCP so the
	// NIC places them in the correct traffic class. For our ma-production
	// RoCEv2 fabric PFC is enabled on the lossless class identified by
	// DSCP 26 (AF31 / class 3). Set to 0 on networks where PFC isn't
	// configured — tagging into a class the switches don't honour can get
	// packets dropped. Override via LONGHORN_V2_REPLICA_TRANSPORT_TOS.
	replicaTransportTos = 26

	// SPDK bdev_nvme invariants enforced by bdev_nvme_check_io_error_resiliency_params:
	//   ctrlr_loss_timeout_sec == 0 requires reconnect_delay_sec == 0 (no retry)
	//   ctrlr_loss_timeout_sec  > 0 requires 0 < reconnect_delay_sec <= ctrlr_loss_timeout_sec
	//   fast_io_fail_timeout_sec (when > 0) must be <= ctrlr_loss_timeout_sec
	//
	// (2, 1, 2) gives the rebuild path a 2 s hard ceiling on retry exposure
	// (one reconnect attempt after 1 s, in-flight IOs fail after 2 s). That is
	// ~7x tighter than the upstream (15, 2, 10) default which at 15 s let a
	// dying peer spam bdev_nvme_failover_ctrlr_unsafe fast enough to saturate
	// the reactor poller and crash spdk_tgt with a broken /var/tmp/spdk.sock.
	// Rebuild is restartable, so a 2 s tolerance for transient RDMA blips is
	// fine and the manager restarts from scratch on harder failures.
	rebuildCtrlrLossTimeoutSec  = 2
	rebuildReconnectDelaySec    = 1
	rebuildFastIOFailTimeoutSec = 2

	// defaultLvolClearMethod controls the clear_method passed to
	// bdev_lvol_create_lvstore and bdev_lvol_create. Empty string means
	// "use SPDK default" (unmap). Longhorn installs running on kernels or
	// bdevs where UNMAP issues synchronous fallocate(PUNCH_HOLE) on the
	// reactor can override to "none" via LONGHORN_V2_LVOL_CLEAR_METHOD.
	defaultLvolClearMethod = ""

	// defaultLvstoreClusterSize controls the cluster_sz passed to
	// bdev_lvol_create_lvstore on new disk registration. The value is fixed
	// at lvstore creation time and cannot be changed; existing disks keep
	// their original cluster size. Larger clusters reduce the per-cluster
	// blob_sync_md cost that caps v2 replica rebuild throughput (upstream
	// SPDK issue #359), at the cost of higher CoW amplification on
	// snapshotted blobs. Override via LONGHORN_V2_LVSTORE_CLUSTER_SIZE
	// (bytes, uint32).
	defaultLvstoreClusterSize uint32 = 1 * 1024 * 1024

	// defaultThinProvision controls the thin_provision flag passed to
	// bdev_lvol_create. true (upstream default) allocates clusters
	// lazily on first write, which triggers a per-cluster spdk_blob_sync_md
	// barrier — a hard serialization point that caps first-write throughput
	// on fresh regions at ~25 IOPS per blob on our hardware (slow mkfs on
	// large volumes, slow rebuild shallow_copy, see SPDK #359). Set to
	// false via LONGHORN_V2_LVOL_THIN_PROVISION=false for installs where
	// the underlying bdev is already thick-allocated (e.g. a fixed-size
	// LVM LV) so the blobstore-level thin tracking adds no capacity
	// savings and only contributes latency.
	defaultThinProvision = true
)

func init() {
	replicaCtrlrLossTimeoutSec = envIntOrDefault("LONGHORN_V2_REPLICA_CTRLR_LOSS_TIMEOUT_SEC", replicaCtrlrLossTimeoutSec)
	replicaReconnectDelaySec = envIntOrDefault("LONGHORN_V2_REPLICA_RECONNECT_DELAY_SEC", replicaReconnectDelaySec)
	replicaFastIOFailTimeoutSec = envIntOrDefault("LONGHORN_V2_REPLICA_FAST_IO_FAIL_TIMEOUT_SEC", replicaFastIOFailTimeoutSec)
	replicaTransportAckTimeout = envIntOrDefault("LONGHORN_V2_REPLICA_TRANSPORT_ACK_TIMEOUT", replicaTransportAckTimeout)
	replicaKeepAliveTimeoutMs = envIntOrDefault("LONGHORN_V2_REPLICA_KEEP_ALIVE_TIMEOUT_MS", replicaKeepAliveTimeoutMs)
	replicaTransportTos = envIntOrDefault("LONGHORN_V2_REPLICA_TRANSPORT_TOS", replicaTransportTos)
	rebuildCtrlrLossTimeoutSec = envIntOrDefault("LONGHORN_V2_REBUILD_CTRLR_LOSS_TIMEOUT_SEC", rebuildCtrlrLossTimeoutSec)
	rebuildFastIOFailTimeoutSec = envIntOrDefault("LONGHORN_V2_REBUILD_FAST_IO_FAIL_TIMEOUT_SEC", rebuildFastIOFailTimeoutSec)
	rebuildReconnectDelaySec = envIntOrDefault("LONGHORN_V2_REBUILD_RECONNECT_DELAY_SEC", rebuildReconnectDelaySec)
	if v, ok := os.LookupEnv("LONGHORN_V2_LVOL_CLEAR_METHOD"); ok {
		defaultLvolClearMethod = strings.TrimSpace(v)
	}
	if v, ok := os.LookupEnv("LONGHORN_V2_LVSTORE_CLUSTER_SIZE"); ok {
		if parsed, err := strconv.ParseUint(strings.TrimSpace(v), 10, 32); err == nil && parsed > 0 {
			defaultLvstoreClusterSize = uint32(parsed)
		}
	}
	if v, ok := os.LookupEnv("LONGHORN_V2_LVOL_THIN_PROVISION"); ok {
		switch strings.ToLower(strings.TrimSpace(v)) {
		case "0", "false", "no", "off":
			defaultThinProvision = false
		case "1", "true", "yes", "on":
			defaultThinProvision = true
		}
	}
}

var (
	// ErrEngineFrontendCreateInvalidArgument indicates the create request carries
	// invalid input, such as an unparsable target address.
	ErrEngineFrontendCreateInvalidArgument = errors.New("engine frontend create invalid argument")
	// ErrEngineFrontendCreatePrecondition indicates the frontend is not in a
	// state that can satisfy create preconditions.
	ErrEngineFrontendCreatePrecondition = errors.New("engine frontend create precondition failed")
	// ErrEngineFrontendLifecyclePrecondition indicates suspend/resume/delete
	// cannot proceed because the frontend is in an incompatible state.
	ErrEngineFrontendLifecyclePrecondition = errors.New("engine frontend lifecycle precondition failed")
	// ErrEngineFrontendLifecycleUnimplemented indicates the requested lifecycle
	// operation is not implemented for the current frontend type.
	ErrEngineFrontendLifecycleUnimplemented = errors.New("engine frontend lifecycle unimplemented")
)

var (
	// ErrRecoverDeviceNotFound indicates the NVMe device was not found on the
	// host during recovery. The persisted record should be removed.
	ErrRecoverDeviceNotFound = errors.New("device not found on host during recovery")
)

var (
	// ErrSwitchOverTargetInvalidInput indicates invalid user input for a target switchover request.
	ErrSwitchOverTargetInvalidInput = errors.New("invalid switchover target request")
	// ErrSwitchOverTargetPrecondition indicates the current frontend state cannot satisfy switchover preconditions.
	ErrSwitchOverTargetPrecondition = errors.New("switchover target precondition failed")
	// ErrSwitchOverTargetEngineNotFound indicates no engine can be resolved from the target side.
	ErrSwitchOverTargetEngineNotFound = errors.New("cannot find target engine for switchover")
	// ErrSwitchOverTargetInternal indicates switchover execution failed due to runtime/internal reasons.
	ErrSwitchOverTargetInternal = errors.New("failed to switch over target")
)

var (
	// ErrExpansionInProgress indicates expansion cannot proceed because another
	// expansion operation is already running.
	ErrExpansionInProgress = errors.New("expansion is in progress")
	// ErrRestoringInProgress indicates expansion cannot proceed while restoring.
	ErrRestoringInProgress = errors.New("restoring is in progress")
	// ErrExpansionInvalidSize indicates an invalid target size for expansion.
	ErrExpansionInvalidSize = errors.New("invalid expansion size")
)

type Lvol struct {
	sync.RWMutex

	Name       string
	UUID       string
	Alias      string
	SpecSize   uint64
	ActualSize uint64
	// Parent is the snapshot lvol name. <snapshot lvol name> consists of `<replica name>-snap-<snapshot name>`
	Parent string
	// Children is map[<snapshot lvol name>] rather than map[<snapshot name>]. <snapshot lvol name> consists of `<replica name>-snap-<snapshot name>`
	Children          map[string]*Lvol
	CreationTime      string
	UserCreated       bool
	SnapshotTimestamp string
	SnapshotChecksum  string
}

func ServiceBackingImageLvolToProtoBackingImageLvol(lvol *Lvol) *spdkrpc.Lvol {
	lvol.RLock()
	defer lvol.RUnlock()

	res := &spdkrpc.Lvol{
		Uuid:       lvol.UUID,
		Name:       lvol.Name,
		SpecSize:   lvol.SpecSize,
		ActualSize: lvol.ActualSize,
		// BackingImage has no parent
		Parent:       "",
		Children:     map[string]bool{},
		CreationTime: lvol.CreationTime,
		UserCreated:  false,
		// Use creation time instead
		SnapshotTimestamp: "",
	}

	for childLvolName := range lvol.Children {
		// For backing image, the children is map[<snapshot lvol name>]
		res.Children[childLvolName] = true
	}

	return res
}

func ServiceLvolToProtoLvol(replicaName string, lvol *Lvol) *spdkrpc.Lvol {
	if lvol == nil {
		return nil
	}
	res := &spdkrpc.Lvol{
		Uuid:              lvol.UUID,
		SpecSize:          lvol.SpecSize,
		ActualSize:        lvol.ActualSize,
		Parent:            GetSnapshotNameFromReplicaSnapshotLvolName(replicaName, lvol.Parent),
		Children:          map[string]bool{},
		CreationTime:      lvol.CreationTime,
		UserCreated:       lvol.UserCreated,
		SnapshotTimestamp: lvol.SnapshotTimestamp,
		SnapshotChecksum:  lvol.SnapshotChecksum,
	}

	if lvol.Name == replicaName {
		res.Name = types.VolumeHead
	} else {
		res.Name = GetSnapshotNameFromReplicaSnapshotLvolName(replicaName, lvol.Name)
	}

	for childLvolName := range lvol.Children {
		// spdkrpc.Lvol.Children is map[<snapshot name>] rather than map[<snapshot lvol name>]
		if childLvolName == replicaName {
			res.Children[types.VolumeHead] = true
		} else {
			res.Children[GetSnapshotNameFromReplicaSnapshotLvolName(replicaName, childLvolName)] = true
		}
	}

	return res
}

func BdevLvolInfoToServiceLvol(bdev *spdktypes.BdevInfo) *Lvol {
	svcLvol := &Lvol{
		Name:              spdktypes.GetLvolNameFromAlias(bdev.Aliases[0]),
		Alias:             bdev.Aliases[0],
		UUID:              bdev.UUID,
		SpecSize:          bdev.NumBlocks * uint64(bdev.BlockSize),
		ActualSize:        bdev.DriverSpecific.Lvol.NumAllocatedClusters * defaultClusterSize,
		Parent:            bdev.DriverSpecific.Lvol.BaseSnapshot,
		Children:          map[string]*Lvol{},
		CreationTime:      bdev.CreationTime,
		UserCreated:       bdev.DriverSpecific.Lvol.Xattrs[spdkclient.UserCreated] == strconv.FormatBool(true),
		SnapshotTimestamp: bdev.DriverSpecific.Lvol.Xattrs[spdkclient.SnapshotTimestamp],
		SnapshotChecksum:  bdev.DriverSpecific.Lvol.Xattrs[spdkclient.SnapshotChecksum],
	}

	// Need to further update this separately
	for _, childLvolName := range bdev.DriverSpecific.Lvol.Clones {
		svcLvol.Children[childLvolName] = nil
	}

	return svcLvol
}

func IsProbablyReplicaName(name string) bool {
	matched, _ := regexp.MatchString("^.+-r-[a-zA-Z0-9]{8}$", name)
	return matched
}

func GetBackingImageSnapLvolName(backingImageName string, lvsUUID string) string {
	return fmt.Sprintf("bi-%s-disk-%s", backingImageName, lvsUUID)
}

func GetBackingImageTempHeadLvolName(backingImageName string, lvsUUID string) string {
	return fmt.Sprintf("bi-%s-disk-%s-temp-head", backingImageName, lvsUUID)
}

func GetReplicaSnapshotLvolNamePrefix(replicaName string) string {
	return fmt.Sprintf("%s-snap-", replicaName)
}

func GetReplicaSnapshotLvolName(replicaName, snapshotName string) string {
	return fmt.Sprintf("%s%s", GetReplicaSnapshotLvolNamePrefix(replicaName), snapshotName)
}

func GetSnapshotNameFromReplicaSnapshotLvolName(replicaName, snapLvolName string) string {
	return strings.TrimPrefix(snapLvolName, GetReplicaSnapshotLvolNamePrefix(replicaName))
}

func IsReplicaLvol(replicaName, lvolName string) bool {
	return strings.HasPrefix(lvolName, fmt.Sprintf("%s-", replicaName)) || lvolName == replicaName
}

func IsReplicaSnapshotLvol(replicaName, lvolName string) bool {
	return strings.HasPrefix(lvolName, GetReplicaSnapshotLvolNamePrefix(replicaName))
}

func GenerateRebuildingSnapshotName() string {
	return fmt.Sprintf("%s-%s", RebuildingSnapshotNamePrefix, util.UUID()[:8])
}

func GenerateReplicaExpiredLvolName(replicaName string) string {
	return fmt.Sprintf("%s-%s-%s", replicaName, ReplicaExpiredLvolSuffix, util.UUID()[:8])
}

func GetReplicaRebuildingLvolName(replicaName string) string {
	return fmt.Sprintf("%s-%s", replicaName, ReplicaRebuildingLvolSuffix)
}

func IsRebuildingLvol(lvolName string) bool {
	return strings.HasSuffix(lvolName, ReplicaRebuildingLvolSuffix)
}

func IsReplicaExpiredLvol(replicaName, lvolName string) bool {
	return strings.HasPrefix(lvolName, fmt.Sprintf("%s-%s", replicaName, ReplicaExpiredLvolSuffix))
}

func GetReplicaNameFromRebuildingLvolName(lvolName string) string {
	return strings.TrimSuffix(lvolName, fmt.Sprintf("-%s", ReplicaRebuildingLvolSuffix))
}

func GetReplicaCloningLvolName(replicaName string) string {
	return fmt.Sprintf("%s-%s", replicaName, ReplicaCloningLvolSuffix)
}

func IsCloningLvol(lvolName string) bool {
	return strings.HasSuffix(lvolName, ReplicaCloningLvolSuffix)
}

func GetReplicaNameFromCloningLvolName(lvolName string) string {
	return strings.TrimSuffix(lvolName, fmt.Sprintf("-%s", ReplicaCloningLvolSuffix))
}

func GetTmpSnapNameForCloningLvol(replicaName string) string {
	return fmt.Sprintf("%s-%s-tmp", replicaName, ReplicaCloningLvolSuffix)
}

func GetNvmfEndpoint(nqn, ip string, port int32) string {
	return fmt.Sprintf("nvmf://%s:%d/%s", ip, port, nqn)
}

func GetServiceClient(address string) (*client.SPDKClient, error) {
	ip, _, err := net.SplitHostPort(address)
	if err != nil {
		return nil, err
	}
	// TODO: Can we use the fixed port
	addr := net.JoinHostPort(ip, strconv.Itoa(types.SPDKServicePort))

	// TODO: Can we share the clients in the whole server?
	return client.NewSPDKClient(addr)
}

func GetBdevMap(cli *spdkclient.Client) (map[string]*spdktypes.BdevInfo, error) {
	bdevList, err := cli.BdevGetBdevs("", 0)
	if err != nil {
		return nil, err
	}

	bdevMap := map[string]*spdktypes.BdevInfo{}
	for idx := range bdevList {
		bdev := &bdevList[idx]
		bdevType := spdktypes.GetBdevType(bdev)

		switch bdevType {
		case spdktypes.BdevTypeLvol:
			if len(bdev.Aliases) != 1 {
				continue
			}
			bdevMap[bdev.Aliases[0]] = bdev
		case spdktypes.BdevTypeRaid:
			fallthrough
		default:
			bdevMap[bdev.Name] = bdev
		}
	}

	return bdevMap, nil
}

func GetBdevLvolMap(cli *spdkclient.Client) (map[string]*spdktypes.BdevInfo, error) {
	return GetBdevLvolMapWithFilter(cli, func(*spdktypes.BdevInfo) bool { return true })
}

func GetBdevLvolMapWithFilter(cli *spdkclient.Client, filter func(*spdktypes.BdevInfo) bool) (map[string]*spdktypes.BdevInfo, error) {
	bdevList, err := cli.BdevLvolGetWithFilter("", 0, filter)
	if err != nil {
		return nil, err
	}

	bdevLvolMap := map[string]*spdktypes.BdevInfo{}
	for idx := range bdevList {
		bdev := &bdevList[idx]
		bdevType := spdktypes.GetBdevType(bdev)
		if bdevType != spdktypes.BdevTypeLvol {
			continue
		}
		if len(bdev.Aliases) != 1 {
			continue
		}
		lvolName := spdktypes.GetLvolNameFromAlias(bdev.Aliases[0])
		bdevLvolMap[lvolName] = bdev
	}

	return bdevLvolMap, nil
}

func GetNvmfSubsystemMap(cli *spdkclient.Client) (map[string]*spdktypes.NvmfSubsystem, error) {
	subsystemList, err := cli.NvmfGetSubsystems("", "")
	if err != nil {
		return nil, err
	}

	subsystemMap := map[string]*spdktypes.NvmfSubsystem{}
	for idx := range subsystemList {
		subsystem := &subsystemList[idx]
		subsystemMap[subsystem.Nqn] = subsystem
	}

	return subsystemMap, nil
}

type BackupCreateInfo struct {
	BackupName     string
	IsIncremental  bool
	ReplicaAddress string
}

func generateNGUID(name string) string {
	nguid := uuid.NewSHA1(uuid.NameSpaceOID, []byte(name))
	return hex.EncodeToString(nguid[:]) // 32-char hex

}

// generateNsUUID creates a deterministic UUID for an NVMe namespace.
// Uses a different UUID namespace (URL) than generateNGUID (OID) to ensure
// the UUID and NGUID values differ while remaining stable for the same input.
func generateNsUUID(name string) string {
	nsUUID := uuid.NewSHA1(uuid.NameSpaceURL, []byte(name))
	return nsUUID.String() // standard UUID format: 8-4-4-4-12
}

func getEngineCntlid(engineName string) uint16 {
	parts := strings.Split(engineName, "-")
	if len(parts) > 0 {
		if ordinal, err := strconv.Atoi(parts[len(parts)-1]); err == nil {
			return uint16(ordinal + 1) // CNTLID must be >= 1
		}
	}
	return 1 // fallback
}

const dualListenerCntlidBase uint16 = 1000
const dualListenerCntlidSlotsPerEngine uint16 = 8

func getEngineDualCntlidRange(engineName string) (uint16, uint16) {
	ordinal := uint16(getEngineCntlid(engineName) - 1)
	lo := dualListenerCntlidBase + ordinal*dualListenerCntlidSlotsPerEngine + 1
	return lo, lo + dualListenerCntlidSlotsPerEngine - 1
}

func envIntOrDefault(name string, def int) int {
	raw, ok := os.LookupEnv(name)
	if !ok || raw == "" {
		return def
	}
	v, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil {
		return def
	}
	return v
}

// defaultRaidDeltaBitmapEnabled returns whether new v2 raid1 bdevs should
// enable per-base-bdev dirty-region tracking. Defaults on; operators can
// force off by setting LONGHORN_V2_RAID_DELTA_BITMAP=0 on the IM pod (e.g.
// if the base bdev layer exposes optimal_io_boundary=0 and would reject
// raid1 startup).
func defaultRaidDeltaBitmapEnabled() bool {
	raw, ok := os.LookupEnv("LONGHORN_V2_RAID_DELTA_BITMAP")
	if !ok {
		return true
	}
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "0", "false", "no", "off":
		return false
	default:
		return true
	}
}
