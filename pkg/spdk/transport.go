package spdk

import (
	"os"
	"strconv"
	"strings"

	"github.com/sirupsen/logrus"

	"github.com/longhorn/go-spdk-helper/pkg/jsonrpc"
	spdkclient "github.com/longhorn/go-spdk-helper/pkg/spdk/client"
	spdktypes "github.com/longhorn/go-spdk-helper/pkg/spdk/types"
)

type NvmfTransportType string

const (
	NvmfTransportTCP  NvmfTransportType = "tcp"
	NvmfTransportRDMA NvmfTransportType = "rdma"
)

const DefaultNvmfTransport = NvmfTransportTCP

func (t NvmfTransportType) ToSPDKTransportType() spdktypes.NvmeTransportType {
	switch t {
	case NvmfTransportRDMA:
		return spdktypes.NvmeTransportTypeRDMA
	default:
		return spdktypes.NvmeTransportTypeTCP
	}
}

func (t NvmfTransportType) IsRDMA() bool { return t == NvmfTransportRDMA }

type TransportCapability struct {
	RDMA bool
}

var infinibandSysfsPath = "/sys/class/infiniband"

func DetectTransport() TransportCapability {
	entries, err := os.ReadDir(infinibandSysfsPath)
	if err != nil {
		if !os.IsNotExist(err) {
			logrus.WithError(err).Debugf("Failed to enumerate RDMA devices at %s", infinibandSysfsPath)
		}
		return TransportCapability{}
	}
	for _, e := range entries {
		if !strings.HasPrefix(e.Name(), ".") {
			return TransportCapability{RDMA: true}
		}
	}
	return TransportCapability{}
}

// Tunables for nvmf_create_transport, all overridable via env. Defaults are:
//
//	MaxQueueDepth=128   — SPDK upstream default. Lower values (e.g. 32) were
//	previously used to mitigate burst saturation that turned out to be caused
//	by NIC adaptive interrupt coalescing + sw_accel data-buffer copy on the
//	reactor; with adaptive coalescing off + accel_mlx5 registered for HW UMR,
//	128 is safe and gives the headroom needed for high-IOPS workloads
//	(16 cores × 128 = 2048 inflight commands per controller, vs only 512 at
//	depth=32). Tune via LONGHORN_V2_NVMF_RDMA_MAX_QUEUE_DEPTH if needed.
//
//	data_wr_pool_size=4095 — critical. SPDK default of 0 forces per-qpair
//	RDMA WR allocation on every submission and caps throughput at hundreds
//	of KB/s. Mayastor uses 4095. Override with
//	LONGHORN_V2_NVMF_RDMA_DATA_WR_POOL_SIZE.
//
// IoUnitSize=8192 is the SPDK-defined RDMA minimum; SPDK chains larger I/Os.
// MaxIoSize=131072 matches kernel's max_hw_sectors_kb.
var (
	nvmfRdmaOpts = spdktypes.NvmfCreateTransportRequest{
		Trtype:              spdktypes.NvmeTransportTypeRDMA,
		MaxQueueDepth:       uint32(envIntOrDefault("LONGHORN_V2_NVMF_RDMA_MAX_QUEUE_DEPTH", 128)),
		MaxIoQpairsPerCtrlr: uint32(envIntOrDefault("LONGHORN_V2_NVMF_RDMA_MAX_IO_QPAIRS_PER_CTRLR", 127)),
		InCapsuleDataSize:   uint32(envIntOrDefault("LONGHORN_V2_NVMF_RDMA_IN_CAPSULE_DATA_SIZE", 4096)),
		MaxIoSize:           uint32(envIntOrDefault("LONGHORN_V2_NVMF_RDMA_MAX_IO_SIZE", 131072)),
		IoUnitSize:          uint32(envIntOrDefault("LONGHORN_V2_NVMF_RDMA_IO_UNIT_SIZE", 8192)),
		MaxAqDepth:          uint32(envIntOrDefault("LONGHORN_V2_NVMF_RDMA_MAX_AQ_DEPTH", 128)),
		NumSharedBuffers:    uint32(envIntOrDefault("LONGHORN_V2_NVMF_RDMA_NUM_SHARED_BUFFERS", 4095)),
		// Explicit per-poll-group iobuf cache caps (v26.05): without them the
		// default cache is pool/(2*poll_groups) PER TRANSPORT, so two
		// transports' caches consume the whole pool at any pool size.
		// buf_cache_size must NOT be sent alongside (shared C-union slot).
		IobufSmallCacheSize: uint32(envIntOrDefault("LONGHORN_V2_NVMF_RDMA_IOBUF_SMALL_CACHE_SIZE", 64)),
		IobufLargeCacheSize: uint32(envIntOrDefault("LONGHORN_V2_NVMF_RDMA_IOBUF_LARGE_CACHE_SIZE", 64)),
		Zcopy:               boolPtr(true),
		DataWrPoolSize:      uint32(envIntOrDefault("LONGHORN_V2_NVMF_RDMA_DATA_WR_POOL_SIZE", 4095)),
		AcceptorPollRate:    uint32(envIntOrDefault("LONGHORN_V2_NVMF_RDMA_ACCEPTOR_POLL_RATE", 10000)),
	}
	// TCP transport opts. Defaults match SPDK upstream — bumping any of
	// these eats DPDK heap that the accel_mlx5 signature/UMR mempools also
	// pull from on RDMA nodes, and we hit ENOMEM during accel module init
	// (subsystem init failed → IM exit) when NumSharedBuffers/BufCacheSize/
	// InCapsuleDataSize were tuned up fleet-wide. Keep env knobs so
	// individual TCP-only nodes can opt back into bigger pools without an
	// IM rebuild — we just won't ship aggressive defaults again.
	nvmfTcpOpts = spdktypes.NvmfCreateTransportRequest{
		Trtype:              spdktypes.NvmeTransportTypeTCP,
		MaxQueueDepth:       uint32(envIntOrDefault("LONGHORN_V2_NVMF_TCP_MAX_QUEUE_DEPTH", 128)),
		MaxIoQpairsPerCtrlr: uint32(envIntOrDefault("LONGHORN_V2_NVMF_TCP_MAX_IO_QPAIRS_PER_CTRLR", 127)),
		InCapsuleDataSize:   uint32(envIntOrDefault("LONGHORN_V2_NVMF_TCP_IN_CAPSULE_DATA_SIZE", 4096)),
		MaxIoSize:           uint32(envIntOrDefault("LONGHORN_V2_NVMF_TCP_MAX_IO_SIZE", 131072)),
		IoUnitSize:          uint32(envIntOrDefault("LONGHORN_V2_NVMF_TCP_IO_UNIT_SIZE", 131072)),
		MaxAqDepth:          uint32(envIntOrDefault("LONGHORN_V2_NVMF_TCP_MAX_AQ_DEPTH", 128)),
		NumSharedBuffers:    uint32(envIntOrDefault("LONGHORN_V2_NVMF_TCP_NUM_SHARED_BUFFERS", 2047)),
		// See the RDMA opts note: explicit caps, and never buf_cache_size.
		IobufSmallCacheSize: uint32(envIntOrDefault("LONGHORN_V2_NVMF_TCP_IOBUF_SMALL_CACHE_SIZE", 64)),
		IobufLargeCacheSize: uint32(envIntOrDefault("LONGHORN_V2_NVMF_TCP_IOBUF_LARGE_CACHE_SIZE", 64)),
		Zcopy:               boolPtr(true),
		AcceptorPollRate:    uint32(envIntOrDefault("LONGHORN_V2_NVMF_TCP_ACCEPTOR_POLL_RATE", 10000)),
	}
)

func boolPtr(b bool) *bool { return &b }

func NegotiateNodeTransport(spdkClient *spdkclient.Client) NvmfTransportType {
	// Pre-create TCP with opts so on-demand ensureNvmfTransport calls later
	// pick it up as already-existing and skip the bare NvmfCreateTransport
	// (which would use pathological defaults).
	if _, err := spdkClient.NvmfCreateTransportWithOpts(nvmfTcpOpts); err != nil && !jsonrpc.IsJSONRPCRespErrorTransportTypeAlreadyExists(err) {
		logrus.WithError(err).Warn("Failed to create NVMe-oF TCP transport with explicit opts; will fall back to SPDK defaults")
	} else {
		logrus.Info("NVMe-oF TCP transport created with tuned opts")
	}
	if !DetectTransport().RDMA {
		return NvmfTransportTCP
	}
	if _, err := spdkClient.NvmfCreateTransportWithOpts(nvmfRdmaOpts); err != nil && !jsonrpc.IsJSONRPCRespErrorTransportTypeAlreadyExists(err) {
		logrus.WithError(err).Warn("SPDK rejected nvmf_create_transport(rdma); falling back to TCP for NVMe-oF")
		return NvmfTransportTCP
	}
	logrus.Info("NVMe-oF RDMA transport negotiated on this node with tuned opts")
	return NvmfTransportRDMA
}

// iobufPoolCounts sizes the iobuf pools from the node's SPDK hugepage
// allocation: the pools are pinned hugepage memory, so sizing them is
// deciding how the allocation splits between I/O buffers and everything
// else (DPDK heap, blobstore metadata, accel mempools). Measured on a busy
// 44-volume engine node (2026-07-06): connection-scaled heap demand (qpair
// in-capsule buffers, RDMA WR pools, mlx5 mempools) was ~4.1GiB of an 8GiB
// budget while iobuf retries stayed at zero — the heap, not the pool, is
// the scarce side. Default: 20% of the
// budget, split 7:1 large:small by bytes (the data path is 128KiB-dominated),
// floored at the SPDK baselines. Overrides:
//
//	LONGHORN_V2_IOBUF_BUDGET_PERCENT     — pool share of the budget (default 20)
//	LONGHORN_V2_IOBUF_LARGE_POOL_COUNT   — absolute large count (trumps all)
//	LONGHORN_V2_IOBUF_SMALL_POOL_COUNT   — absolute small count (trumps all)
//
// When the budget is unknowable the configured-demand fallback applies:
// SPDK baselines + each created transport's num_shared_buffers + the capped
// per-poll-group caches + a per-reactor channel allowance.
// Transport caches MUST be explicitly capped (IobufSmall/LargeCacheSize in
// the opts): the v26.05 default cache is pool/(2*poll_groups) per transport,
// which makes demand scale with the pool itself and never converge.
func iobufPoolCounts(rdmaCapable bool, reactors int, budgetBytes uint64) (small, large uint64) {
	if reactors < 1 {
		reactors = 1
	}

	if budgetBytes > 0 {
		pct := envIntOrDefault("LONGHORN_V2_IOBUF_BUDGET_PERCENT", 20)
		if pct < 1 || pct > 90 {
			pct = 20
		}
		poolBytes := budgetBytes * uint64(pct) / 100
		// 7:1 large:small by bytes.
		large = poolBytes * 7 / 8 / iobufLargeBufsize
		small = poolBytes / 8 / iobufSmallBufsize
	} else {
		// Configured-demand fallback (budget unknown).
		fr := uint64(reactors)
		const chanLargePerReactor = 64
		const chanSmallPerReactor = 128
		large = iobufBaseLargePoolCount +
			uint64(nvmfTcpOpts.NumSharedBuffers) +
			uint64(nvmfTcpOpts.IobufLargeCacheSize)*fr +
			chanLargePerReactor*fr
		small = iobufBaseSmallPoolCount +
			uint64(nvmfTcpOpts.IobufSmallCacheSize)*fr +
			chanSmallPerReactor*fr
		if rdmaCapable {
			large += uint64(nvmfRdmaOpts.NumSharedBuffers) +
				uint64(nvmfRdmaOpts.IobufLargeCacheSize)*fr
			small += uint64(nvmfRdmaOpts.IobufSmallCacheSize) * fr
		}
	}

	// Floors: the SPDK baselines PLUS the transports' capped per-poll-group
	// cache populations — a pool smaller than base+caches re-creates the
	// init-time starvation the caps exist to prevent.
	r := uint64(reactors)
	largeFloor := iobufBaseLargePoolCount + uint64(nvmfTcpOpts.IobufLargeCacheSize)*r
	smallFloor := iobufBaseSmallPoolCount + uint64(nvmfTcpOpts.IobufSmallCacheSize)*r
	if rdmaCapable {
		largeFloor += uint64(nvmfRdmaOpts.IobufLargeCacheSize) * r
		smallFloor += uint64(nvmfRdmaOpts.IobufSmallCacheSize) * r
	}
	if large < largeFloor {
		large = largeFloor
	}
	if small < smallFloor {
		small = smallFloor
	}
	if v := envIntOrDefault("LONGHORN_V2_IOBUF_LARGE_POOL_COUNT", 0); v > 0 {
		large = uint64(v)
	}
	if v := envIntOrDefault("LONGHORN_V2_IOBUF_SMALL_POOL_COUNT", 0); v > 0 {
		small = uint64(v)
	}
	return small, large
}

// iobufPoolBytes is the hugepage memory the derived pools will pin.
func iobufPoolBytes(small, large uint64) uint64 {
	return small*iobufSmallBufsize + large*iobufLargeBufsize
}

// spdkMemSizeBytes returns the node's SPDK hugepage allocation: the
// LONGHORN_V2_SPDK_MEM_SIZE_MIB env exported by the IM wrapper from the
// --mem-size argument (the Longhorn spdk-memory-size setting / per-node
// label), falling back to the host's total 2MiB hugepage reservation from
// /proc/meminfo. Returns 0 when neither is available.
func spdkMemSizeBytes() uint64 {
	if v := envIntOrDefault("LONGHORN_V2_SPDK_MEM_SIZE_MIB", 0); v > 0 {
		return uint64(v) << 20
	}
	data, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		return 0
	}
	var totalPages, pageKiB uint64
	for _, line := range strings.Split(string(data), "\n") {
		f := strings.Fields(line)
		if len(f) < 2 {
			continue
		}
		switch f[0] {
		case "HugePages_Total:":
			totalPages = parseUintOrZero(f[1])
		case "Hugepagesize:":
			pageKiB = parseUintOrZero(f[1])
		}
	}
	return totalPages * pageKiB << 10
}

func parseUintOrZero(s string) uint64 {
	v, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0
	}
	return v
}
