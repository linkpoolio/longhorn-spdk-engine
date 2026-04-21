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

func withInfinibandSysfs(t *testing.T, path string) {
	t.Helper()
	orig := infinibandSysfsPath
	t.Cleanup(func() { infinibandSysfsPath = orig })
	infinibandSysfsPath = path
}

func TestDetectTransportNoInfiniband(t *testing.T) {
	withInfinibandSysfs(t, filepath.Join(t.TempDir(), "nonexistent"))
	if DetectTransport().RDMA {
		t.Errorf("expected RDMA=false when sysfs path missing")
	}
}

func TestDetectTransportEmptyInfiniband(t *testing.T) {
	withInfinibandSysfs(t, t.TempDir())
	if DetectTransport().RDMA {
		t.Errorf("expected RDMA=false when sysfs path empty")
	}
}

func TestDetectTransportWithDevice(t *testing.T) {
	dir := t.TempDir()
	if err := os.Mkdir(filepath.Join(dir, "mlx5_0"), 0755); err != nil {
		t.Fatal(err)
	}
	withInfinibandSysfs(t, dir)
	if !DetectTransport().RDMA {
		t.Errorf("expected RDMA=true when device present")
	}
}

func TestDetectTransportIgnoresDotEntries(t *testing.T) {
	dir := t.TempDir()
	if err := os.Mkdir(filepath.Join(dir, ".keep"), 0755); err != nil {
		t.Fatal(err)
	}
	withInfinibandSysfs(t, dir)
	if DetectTransport().RDMA {
		t.Errorf("expected RDMA=false when only dot entries present")
	}
}
