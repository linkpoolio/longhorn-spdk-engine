package spdk

import (
	"encoding/hex"
	"fmt"
	"net"
	"os"
	"regexp"
	"runtime"
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
	// MaxShallowCopyStallTime is how long an in-progress shallow copy may report
	// the same handled-cluster count before the rebuild is aborted. It bounds the
	// brownout case MaxShallowCopyWaitTime never catches: the status RPCs keep
	// succeeding but the copy itself is frozen.
	MaxShallowCopyStallTime = 5 * time.Minute

	MaxSnapshotCloneWaitTime         = 72 * time.Hour
	SnapshotCloneStatusCheckInterval = 3 * time.Second
)

const (
	replicaMultipath = "disable"
)

// Replica NVMe-oF timeouts. Kept as vars (not consts) so they can be overridden
// per-IM via env vars set by longhorn-manager's instance_manager_controller from
// the data-engine Setting CRs; all existing bare-identifier call sites still compile.
//
// replicaCtrlrLossTimeoutSec lowered from upstream 15s to 3s: when a remote
// replica IM disappears mid-rebuild, every RDMA_CM_EVENT_REJECTED from the
// dying peer triggers bdev_nvme_failover_ctrlr reactively (no cooldown),
// starving the local reactor until ctrlr_loss fires and the controller is
// reaped. The longer the timeout, the more sustained the failover spam; at
// upstream 15s the SPDK reactor can saturate enough to break its own JSONRPC
// socket. 3s trims the spam window below the liveness threshold.
//
// SPDK requires reconnect_delay_sec <= fast_io_fail_timeout_sec <= ctrlr_loss_timeout_sec
// (rpc_bdev_nvme_attach_controller validation in bdev_nvme.c); an out-of-order
// tuple is rejected at attach time. fast_io_fail must therefore track the 3s
// loss timeout. It was previously 10 — invalid against loss=3, and only ever
// worked because longhorn-manager's env override below replaced all three with a
// consistent (but unintentionally 15s) tuple from the Setting CRs. With the
// override absent or partial, loss=3/fast_io_fail=10 fails every replica attach.
//
// rebuildCtrlrLossTimeoutSec / rebuildFastIOFailTimeoutSec apply only to the
// rebuild-path bdev_nvme attachments in replica.go. Rebuild is inherently
// restartable, so sub-second failover is safe and makes teardown-during-rebuild
// crash-proof.
var (
	replicaCtrlrLossTimeoutSec  = 3
	replicaReconnectDelaySec    = 2
	replicaFastIOFailTimeoutSec = 2
	replicaTransportAckTimeout  = 10
	replicaKeepAliveTimeoutMs   = 10000
	// replicaTransportTos tags outbound NVMe-oF packets with DSCP. SPDK passes
	// this byte to rdma_set_option(RDMA_OPTION_ID_TOS), the raw 8-bit IPv4 TOS
	// (DSCP in the upper 6 bits). DSCP 26 (AF31) = TOS 26<<2 = 104. Set 0 where
	// PFC isn't configured. Override via LONGHORN_V2_REPLICA_TRANSPORT_TOS.
	replicaTransportTos = 104

	// iobuf pool sizes. SPDK defaults are too small once nvmf transports use a
	// tuned num_shared_buffers; sized for that + accel/bdev channel caches.
	iobufLargePoolCount uint64 = 4096
	iobufSmallPoolCount uint64 = 8192

	// accelMlx5MkeysPerCore is the per-core scaling factor for accel_mlx5's mkey
	// pool. SPDK enforces a minimum of ACCEL_MLX5_MAX_MKEYS_IN_TASK(16) per core;
	// the upstream 2047 total can ENOMEM on ConnectX firmware that advertises
	// crc32c but can't back that many PSVs. 64/core scales with the pinned cores.
	accelMlx5MkeysPerCore uint32 = 64

	// Rebuild-path bdev_nvme timeouts. Previously (2,1,2) to cap reactor-
	// saturation exposure from failover spam against a dying peer. That made a
	// SLOW-but-alive rebuild source (e.g. a multi-TiB pre-rebase node feeding
	// ~96 concurrent rebuilds) get declared dead at 2s, and since the rebuild
	// source is often the volume's only healthy replica, the connection failure
	// cascaded into a faulted volume (incident 2026-06-15). The reactor-
	// saturation concern is now handled at the SPDK layer (bdev_nvme failover
	// re-drive is rate-limited to ~1 Hz, linkpool.23), so these can be generous
	// enough to ride out a slow source without declaring it dead. Override via
	// the LONGHORN_V2_REBUILD_* env vars below.
	rebuildCtrlrLossTimeoutSec  = 30
	rebuildReconnectDelaySec    = 5
	rebuildFastIOFailTimeoutSec = 15

	// defaultLvolClearMethod is the clear_method passed to bdev_lvol_create[_lvstore].
	// "" = SPDK default (unmap). Installs where UNMAP issues synchronous
	// fallocate(PUNCH_HOLE) on the reactor can override to "none" via
	// LONGHORN_V2_LVOL_CLEAR_METHOD.
	defaultLvolClearMethod = ""

	// defaultLvstoreClusterSize is the cluster_sz for new lvstores (fixed at
	// creation). Larger clusters cut the per-cluster blob_sync_md cost that caps
	// v2 rebuild throughput (SPDK #359). Override via LONGHORN_V2_LVSTORE_CLUSTER_SIZE.
	defaultLvstoreClusterSize uint32 = 1 * 1024 * 1024

	// defaultThinProvision is the thin_provision flag for bdev_lvol_create. true
	// (upstream) allocates lazily, triggering a per-cluster spdk_blob_sync_md
	// barrier that caps first-write throughput. Set false via
	// LONGHORN_V2_LVOL_THIN_PROVISION=false when the bdev is already thick.
	defaultThinProvision = true
)

func init() {
	replicaCtrlrLossTimeoutSec = envIntOrDefault("LONGHORN_V2_REPLICA_CTRLR_LOSS_TIMEOUT_SEC", replicaCtrlrLossTimeoutSec)
	replicaReconnectDelaySec = envIntOrDefault("LONGHORN_V2_REPLICA_RECONNECT_DELAY_SEC", replicaReconnectDelaySec)
	replicaFastIOFailTimeoutSec = envIntOrDefault("LONGHORN_V2_REPLICA_FAST_IO_FAIL_TIMEOUT_SEC", replicaFastIOFailTimeoutSec)
	replicaTransportAckTimeout = envIntOrDefault("LONGHORN_V2_REPLICA_TRANSPORT_ACK_TIMEOUT", replicaTransportAckTimeout)
	replicaKeepAliveTimeoutMs = envIntOrDefault("LONGHORN_V2_REPLICA_KEEP_ALIVE_TIMEOUT_MS", replicaKeepAliveTimeoutMs)
	replicaTransportTos = envIntOrDefault("LONGHORN_V2_REPLICA_TRANSPORT_TOS", replicaTransportTos)
	if v := envIntOrDefault("LONGHORN_V2_IOBUF_LARGE_POOL_COUNT", int(iobufLargePoolCount)); v > 0 {
		iobufLargePoolCount = uint64(v)
	}
	if v := envIntOrDefault("LONGHORN_V2_IOBUF_SMALL_POOL_COUNT", int(iobufSmallPoolCount)); v > 0 {
		iobufSmallPoolCount = uint64(v)
	}
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

	// longhorn-manager sets each timeout from a separate Setting CR, so a partial
	// change (e.g. lowering ctrlr_loss without lowering fast_io_fail) can yield a
	// tuple SPDK rejects at attach (reconnect_delay <= fast_io_fail <= ctrlr_loss),
	// failing every replica/rebuild attach. Clamp to the ordering as a safety net.
	replicaCtrlrLossTimeoutSec, replicaReconnectDelaySec, replicaFastIOFailTimeoutSec =
		enforceAttachTimeoutOrder("replica", replicaCtrlrLossTimeoutSec, replicaReconnectDelaySec, replicaFastIOFailTimeoutSec)
	rebuildCtrlrLossTimeoutSec, rebuildReconnectDelaySec, rebuildFastIOFailTimeoutSec =
		enforceAttachTimeoutOrder("rebuild", rebuildCtrlrLossTimeoutSec, rebuildReconnectDelaySec, rebuildFastIOFailTimeoutSec)
}

// enforceAttachTimeoutOrder clamps an NVMe-oF reconnect tuple to SPDK's
// rpc_bdev_nvme_attach_controller constraint
// reconnect_delay_sec <= fast_io_fail_timeout_sec <= ctrlr_loss_timeout_sec.
// Only a finite positive ctrlr_loss is bounded this way; loss == -1 (retry
// forever) and loss == 0 follow other SPDK rules and are returned untouched.
// A clamp means a Setting was misconfigured, so warn loudly (to stderr, since
// this runs at package init before logging is wired up).
func enforceAttachTimeoutOrder(name string, loss, reconnect, fastfail int) (int, int, int) {
	if loss <= 0 {
		return loss, reconnect, fastfail
	}
	origReconnect, origFastfail := reconnect, fastfail
	if fastfail > loss {
		fastfail = loss
	}
	if fastfail > 0 && reconnect > fastfail {
		reconnect = fastfail
	}
	if reconnect > loss {
		reconnect = loss
	}
	if reconnect < 1 {
		reconnect = 1
	}
	if reconnect != origReconnect || fastfail != origFastfail {
		fmt.Fprintf(os.Stderr, "spdk: clamped %s NVMe-oF timeouts to SPDK attach ordering: "+
			"ctrlr_loss_timeout_sec=%d reconnect_delay_sec=%d fast_io_fail_timeout_sec=%d "+
			"(was reconnect_delay_sec=%d fast_io_fail_timeout_sec=%d)\n",
			name, loss, reconnect, fastfail, origReconnect, origFastfail)
	}
	return loss, reconnect, fastfail
}

// accelMlx5NumRequests sizes the per-device mkey pool for the accel_mlx5 scan.
// SPDK enforces num_requests/cores >= ACCEL_MLX5_MAX_MKEYS_IN_TASK(16), where
// "cores" is spdk_env_get_core_count() (the SPDK cpumask's bit count). The IM
// wrapper exports LONGHORN_V2_SPDK_CPUMASK; we count its bits. Override with
// LONGHORN_V2_ACCEL_MLX5_NUM_REQUESTS.
func accelMlx5NumRequests() uint32 {
	cores := spdkCoreCount()
	n := uint32(cores) * accelMlx5MkeysPerCore
	if v := envIntOrDefault("LONGHORN_V2_ACCEL_MLX5_NUM_REQUESTS", int(n)); v > 0 {
		n = uint32(v)
	}
	return n
}

// spdkCoreCount counts bits in LONGHORN_V2_SPDK_CPUMASK (set by the IM wrapper
// from --spdk-cpumask), matching spdk_env_get_core_count() inside spdk_tgt. Hex,
// optionally 0x-prefixed. Falls back to runtime.NumCPU() when unset.
func spdkCoreCount() int {
	mask := strings.TrimSpace(os.Getenv("LONGHORN_V2_SPDK_CPUMASK"))
	if mask == "" {
		c := runtime.NumCPU()
		if c < 1 {
			c = 1
		}
		return c
	}
	mask = strings.TrimPrefix(strings.TrimPrefix(mask, "0x"), "0X")
	v, err := strconv.ParseUint(mask, 16, 64)
	if err != nil || v == 0 {
		c := runtime.NumCPU()
		if c < 1 {
			c = 1
		}
		return c
	}
	count := 0
	for ; v != 0; v >>= 1 {
		if v&1 == 1 {
			count++
		}
	}
	return count
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
	// ErrRecoveryCancelled indicates that recovery was aborted because a
	// concurrent operation (e.g. EngineFrontendCreate) changed the ef state
	// from Pending, meaning host-level operations should not proceed.
	ErrRecoveryCancelled = errors.New("recovery cancelled by concurrent operation")
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
	// ErrAlreadyRestored indicates the requested backup has already been restored.
	ErrAlreadyRestored = errors.New("already restored backup")
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

// getEngineCntlid derives a unique NVMe controller ID from the engine name.
// Engine names have the format "{volumeName}-e-{ordinal}", where ordinal is
// 0, 1, 2, etc. The CNTLID must be unique per subsystem NQN to avoid
// "Duplicate cntlid" errors when the host connects to multiple SPDK targets
// sharing the same NQN for NVMe multipath.
func getEngineCntlid(engineName string) uint16 {
	parts := strings.Split(engineName, "-")
	if len(parts) > 0 {
		if ordinal, err := strconv.Atoi(parts[len(parts)-1]); err == nil {
			return uint16(ordinal + 1) // CNTLID must be >= 1
		}
	}
	return 1 // fallback
}

// CNTLID range allocation for an engine's NVMe-oF subsystem.
//
// SPDK enforces (maxCntlid - minCntlid + 1) as the maximum number of
// controllers a subsystem may hold at once. A subsystem must have room for the
// one live host plus the controllers that transiently pile up while a kernel
// initiator reconnects: it retries every --reconnect-delay seconds while a
// stale controller is only reaped on keep-alive timeout. If the window is too
// small a recoverable disconnect becomes a permanent wedge — SPDK logs
// "Reached max simultaneous ctrlrs" and every subsequent connect is rejected —
// so each window is deliberately large.
//
// At most two targets expose the same NQN simultaneously: the two sides of a
// live migration / engine upgrade. GenerateEngineNameForVolume increments the
// engine ordinal on every replacement, so those two targets always carry
// *consecutive* ordinals. Mapping each ordinal to a distinct window
// (ordinal mod cntlidWindowSlots, which must be >= 2) guarantees the two
// concurrent targets never share a cntlid. Every window starts at
// cntlidRangeBase, above the legacy pre-windowing scheme (cntlid..cntlid+3,
// near 1), so a rolling upgrade from an old binary stays disjoint as well.
//
// Bounds check: the highest window is cntlidRangeBase + (cntlidWindowSlots-1)*
// cntlidWindowSize + cntlidWindowSize = 1000 + 3*16000 + 16000 = 65000, within
// the uint16 / SPDK valid cntlid space (<= 0xffef).
const (
	cntlidRangeBase   uint16 = 1000
	cntlidWindowSize  uint16 = 16000
	cntlidWindowSlots uint16 = 4
)

func getEngineCntlidRange(engineName string) (uint16, uint16) {
	slot := (getEngineCntlid(engineName) - 1) % cntlidWindowSlots
	lo := cntlidRangeBase + slot*cntlidWindowSize + 1
	return lo, lo + cntlidWindowSize - 1
}

// envIntOrDefault reads an integer tunable from the environment, falling back
// to def when unset, empty, or unparseable. Used by the transport/SPDK opts
// tuning so operators can override defaults per IM pod without a rebuild.
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
