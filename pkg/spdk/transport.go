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

func NegotiateNodeTransport(spdkClient *spdkclient.Client) NvmfTransportType {
	if !DetectTransport().RDMA {
		return NvmfTransportTCP
	}
	if _, err := spdkClient.NvmfCreateTransport(spdktypes.NvmeTransportTypeRDMA); err != nil && !jsonrpc.IsJSONRPCRespErrorTransportTypeAlreadyExists(err) {
		logrus.WithError(err).Warn("SPDK rejected nvmf_create_transport(rdma); falling back to TCP for NVMe-oF")
		return NvmfTransportTCP
	}
	logrus.Info("NVMe-oF RDMA transport negotiated on this node")
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
				if _, err := spdkClient.NvmfCreateTransport(spdktypes.NvmeTransportTypeRDMA); err != nil && !jsonrpc.IsJSONRPCRespErrorTransportTypeAlreadyExists(err) {
					continue
				}
				if rdmaReprobeLogged.CompareAndSwap(false, true) {
					logrus.Warn("RDMA transport is now available on this node but engines are running with TCP; restart the instance-manager pod to migrate to RDMA")
				}
			}
		}
	}()
}
