package spdk

import (
	"context"
	"os"
	"strings"
	"sync/atomic"
	"time"

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

const defaultTransportReprobeInterval = 60 * time.Second

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
//   MaxQueueDepth=128   — SPDK upstream default. Lower values (e.g. 32) were
//   previously used to mitigate burst saturation that turned out to be caused
//   by NIC adaptive interrupt coalescing + sw_accel data-buffer copy on the
//   reactor; with adaptive coalescing off + accel_mlx5 registered for HW UMR,
//   128 is safe and gives the headroom needed for high-IOPS workloads
//   (16 cores × 128 = 2048 inflight commands per controller, vs only 512 at
//   depth=32). Tune via LONGHORN_V2_NVMF_RDMA_MAX_QUEUE_DEPTH if needed.
//
//   data_wr_pool_size=4095 — critical. SPDK default of 0 forces per-qpair
//   RDMA WR allocation on every submission and caps throughput at hundreds
//   of KB/s. Mayastor uses 4095. Override with
//   LONGHORN_V2_NVMF_RDMA_DATA_WR_POOL_SIZE.
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
		BufCacheSize:        uint32(envIntOrDefault("LONGHORN_V2_NVMF_RDMA_BUF_CACHE_SIZE", 64)),
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
		BufCacheSize:        uint32(envIntOrDefault("LONGHORN_V2_NVMF_TCP_BUF_CACHE_SIZE", 64)),
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

var rdmaReprobeLogged atomic.Bool

func StartTransportReprobe(ctx context.Context, spdkClient *spdkclient.Client, negotiated NvmfTransportType) {
	if negotiated == NvmfTransportRDMA {
		return
	}
	go func() {
		ticker := time.NewTicker(defaultTransportReprobeInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if !DetectTransport().RDMA {
					continue
				}
				if _, err := spdkClient.NvmfCreateTransportWithOpts(nvmfRdmaOpts); err != nil && !jsonrpc.IsJSONRPCRespErrorTransportTypeAlreadyExists(err) {
					continue
				}
				if rdmaReprobeLogged.CompareAndSwap(false, true) {
					logrus.Warn("RDMA transport is now available on this node but engines are running with TCP; restart the instance-manager pod to migrate to RDMA")
				}
			}
		}
	}()
}
