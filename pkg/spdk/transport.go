package spdk

import (
	"os"
	"path/filepath"
	"strings"

	"github.com/sirupsen/logrus"

	spdktypes "github.com/longhorn/go-spdk-helper/pkg/spdk/types"
)

// NvmfTransportType is the Longhorn-side view of an NVMe-oF transport.
// It maps 1:1 to spdktypes.NvmeTransportType but carries Longhorn semantics
// (engine selection, ANA preference, frontend dispatch).
type NvmfTransportType string

const (
	NvmfTransportTCP  NvmfTransportType = "tcp"
	NvmfTransportRDMA NvmfTransportType = "rdma"
)

// DefaultNvmfTransport is used by call sites that have not been threaded
// with an explicit transport yet. All existing v2 volumes and legacy call
// paths resolve to TCP so behavior is unchanged by default.
const DefaultNvmfTransport = NvmfTransportTCP

// ToSPDKTransportType converts the Longhorn transport enum to the SPDK
// wire-level constant understood by go-spdk-helper and the SPDK JSON-RPC.
func (t NvmfTransportType) ToSPDKTransportType() spdktypes.NvmeTransportType {
	switch t {
	case NvmfTransportRDMA:
		return spdktypes.NvmeTransportTypeRDMA
	default:
		return spdktypes.NvmeTransportTypeTCP
	}
}

// String returns the lowercase wire string, matching what nvme-cli, SPDK
// JSON-RPC, and kernel sysfs all expect.
func (t NvmfTransportType) String() string { return string(t) }

// IsRDMA reports whether the transport requires RDMA-specific handling
// (explicit QP teardown, MR pinning, module checks).
func (t NvmfTransportType) IsRDMA() bool { return t == NvmfTransportRDMA }

// TransportCapability captures what a node can do. It is populated once at
// instance-manager startup and consumed by frontend setup, scheduler hints,
// and the k8s node label.
type TransportCapability struct {
	// RDMA is true when the kernel exposes at least one RDMA device under
	// /sys/class/infiniband and the required userspace bits (ibverbs) are
	// present. This does NOT guarantee a working fabric: link down, PFC
	// misconfig, or soft-RoCE without rdma_rxe will still make RDMA fail
	// at connection time. Capability detection is a coarse gate; the final
	// decision to start an RDMA listener happens at engine startup and
	// falls back to TCP on any failure.
	RDMA bool
	// TCP is always true: every Linux node with nvme_tcp loaded can serve
	// TCP, and nvme_tcp is already a hard requirement of v2 today.
	TCP bool
}

// infinibandSysfsPath is the canonical location where the kernel enumerates
// RDMA devices. Exposed as a var so tests can point it at a fixture tree.
var infinibandSysfsPath = "/sys/class/infiniband"

// DetectTransport returns the best-effort capability set for this node.
//
// Detection intentionally errs toward TCP-only: if anything about the RDMA
// probe is ambiguous (sysfs missing, read error, empty device list) we
// return RDMA=false and let callers fall back. The only way RDMA=true is
// emitted is when at least one device directory is present under
// /sys/class/infiniband.
func DetectTransport() TransportCapability {
	cap := TransportCapability{TCP: true}

	entries, err := os.ReadDir(infinibandSysfsPath)
	if err != nil {
		if !os.IsNotExist(err) {
			logrus.WithError(err).Debugf("Failed to enumerate RDMA devices at %s, assuming no RDMA", infinibandSysfsPath)
		}
		return cap
	}

	for _, e := range entries {
		name := e.Name()
		if name == "" || strings.HasPrefix(name, ".") {
			continue
		}
		// Presence of any device entry (including soft-RoCE rxe0, mlx5_0, etc.)
		// is enough to flip the flag on. ibverbs health is verified later when
		// the SPDK engine tries to create the transport.
		if _, err := os.Stat(filepath.Join(infinibandSysfsPath, name)); err == nil {
			cap.RDMA = true
			return cap
		}
	}

	return cap
}

// PreferredListenerTransport picks the transport an engine should advertise
// as ANA-optimized for new connections. RDMA wins when available; TCP is the
// unconditional fallback.
func (c TransportCapability) PreferredListenerTransport() NvmfTransportType {
	if c.RDMA {
		return NvmfTransportRDMA
	}
	return NvmfTransportTCP
}
