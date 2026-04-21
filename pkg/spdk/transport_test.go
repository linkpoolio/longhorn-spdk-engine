package spdk

import (
	"os"
	"path/filepath"
	"testing"

	spdktypes "github.com/longhorn/go-spdk-helper/pkg/spdk/types"
)

func TestNvmfTransportTypeMapping(t *testing.T) {
	cases := []struct {
		in  NvmfTransportType
		out spdktypes.NvmeTransportType
	}{
		{NvmfTransportRDMA, spdktypes.NvmeTransportTypeRDMA},
		{NvmfTransportTCP, spdktypes.NvmeTransportTypeTCP},
		{NvmfTransportType(""), spdktypes.NvmeTransportTypeTCP},
		{NvmfTransportType("bogus"), spdktypes.NvmeTransportTypeTCP},
	}
	for _, c := range cases {
		if got := c.in.ToSPDKTransportType(); got != c.out {
			t.Errorf("ToSPDKTransportType(%q) = %q, want %q", c.in, got, c.out)
		}
	}
}

func TestNvmfTransportTypeIsRDMA(t *testing.T) {
	if !NvmfTransportRDMA.IsRDMA() {
		t.Errorf("NvmfTransportRDMA.IsRDMA() should be true")
	}
	if NvmfTransportTCP.IsRDMA() {
		t.Errorf("NvmfTransportTCP.IsRDMA() should be false")
	}
}

func TestDetectTransportNoInfiniband(t *testing.T) {
	dir := t.TempDir()
	orig := infinibandSysfsPath
	defer func() { infinibandSysfsPath = orig }()
	infinibandSysfsPath = filepath.Join(dir, "nonexistent")

	cap := DetectTransport()
	if cap.RDMA {
		t.Errorf("expected RDMA=false when sysfs path missing, got true")
	}
	if !cap.TCP {
		t.Errorf("expected TCP=true always")
	}
	if got := cap.PreferredListenerTransport(); got != NvmfTransportTCP {
		t.Errorf("PreferredListenerTransport = %q, want TCP", got)
	}
}

func TestDetectTransportEmptyInfiniband(t *testing.T) {
	dir := t.TempDir()
	orig := infinibandSysfsPath
	defer func() { infinibandSysfsPath = orig }()
	infinibandSysfsPath = dir

	cap := DetectTransport()
	if cap.RDMA {
		t.Errorf("expected RDMA=false when sysfs path empty, got true")
	}
}

func TestDetectTransportWithDevice(t *testing.T) {
	dir := t.TempDir()
	if err := os.Mkdir(filepath.Join(dir, "mlx5_0"), 0755); err != nil {
		t.Fatal(err)
	}
	orig := infinibandSysfsPath
	defer func() { infinibandSysfsPath = orig }()
	infinibandSysfsPath = dir

	cap := DetectTransport()
	if !cap.RDMA {
		t.Errorf("expected RDMA=true when device present, got false")
	}
	if got := cap.PreferredListenerTransport(); got != NvmfTransportRDMA {
		t.Errorf("PreferredListenerTransport = %q, want RDMA", got)
	}
}

func TestDetectTransportIgnoresDotEntries(t *testing.T) {
	dir := t.TempDir()
	if err := os.Mkdir(filepath.Join(dir, ".keep"), 0755); err != nil {
		t.Fatal(err)
	}
	orig := infinibandSysfsPath
	defer func() { infinibandSysfsPath = orig }()
	infinibandSysfsPath = dir

	cap := DetectTransport()
	if cap.RDMA {
		t.Errorf("expected RDMA=false when only dot entries present, got true")
	}
}
