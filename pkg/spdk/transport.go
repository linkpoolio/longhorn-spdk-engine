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

// Tunables for nvmf_create_transport. Values follow Mayastor's proven set
// for RDMA on RoCEv2/PFC fabrics with SPDK upstream defaults as the floor
// elsewhere. data_wr_pool_size is the critical one: SPDK's default of 0
// forces per-qpair RDMA WR allocation on every submission and caps
// throughput at hundreds of KB/s; 4095 matches num_shared_buffers.
var (
	nvmfRdmaOpts = spdktypes.NvmfCreateTransportRequest{
		Trtype:              spdktypes.NvmeTransportTypeRDMA,
		MaxQueueDepth:       128,
		MaxIoQpairsPerCtrlr: 127,
		InCapsuleDataSize:   4096,
		MaxIoSize:           131072,
		IoUnitSize:          8192, // SPDK-defined RDMA minimum; larger is chained by SPDK anyway
		MaxAqDepth:          128,
		NumSharedBuffers:    4095,
		BufCacheSize:        64,
		Zcopy:               boolPtr(true),
		DataWrPoolSize:      4095,
		AcceptorPollRate:    10000,
	}
	nvmfTcpOpts = spdktypes.NvmfCreateTransportRequest{
		Trtype:              spdktypes.NvmeTransportTypeTCP,
		MaxQueueDepth:       128,
		MaxIoQpairsPerCtrlr: 127,
		InCapsuleDataSize:   4096,
		MaxIoSize:           131072,
		IoUnitSize:          131072,
		MaxAqDepth:          128,
		NumSharedBuffers:    4095,
		BufCacheSize:        64,
		Zcopy:               boolPtr(true),
		AcceptorPollRate:    10000,
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
