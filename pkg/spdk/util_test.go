package spdk

import (
	"fmt"
	"net"
	"testing"
	"time"

	. "gopkg.in/check.v1"

	"github.com/longhorn/longhorn-spdk-engine/pkg/types"

	spdkclient "github.com/longhorn/go-spdk-helper/pkg/spdk/client"
	spdktypes "github.com/longhorn/go-spdk-helper/pkg/spdk/types"
)

func Test(t *testing.T) { TestingT(t) }

type TestSuite struct{}

var _ = Suite(&TestSuite{})

// The port-allocation preflight test-binds on the instance's IP, which in
// unit tests is a fabricated address that can never bind. Neutralize it by
// default; TestAllocateUsablePortRange installs its own fake and restores.
func (s *TestSuite) SetUpTest(c *C) {
	testPortBindFn = func(string, int32) error { return nil }
}

const defaultTestSnapshotMaxCount int32 = 2

func (s *TestSuite) TestSplitHostPort(c *C) {
	fmt.Println("Testing splitHostPort with various address formats")

	type testCase struct {
		address      string
		expectedHost string
		expectedPort int32
		expectedErr  error
	}
	testCases := map[string]testCase{
		"splitHostPort(...): normal address": {
			address:      "127.0.0.1:8080",
			expectedHost: "127.0.0.1",
			expectedPort: 8080,
			expectedErr:  nil,
		},
		"splitHostPort(...): only host": {
			address:      "127.0.0.1",
			expectedHost: "127.0.0.1",
			expectedPort: 0,
			expectedErr:  nil,
		},
		"splitHostPort(...): only port": {
			address:      ":8080",
			expectedHost: "",
			expectedPort: 8080,
			expectedErr:  nil,
		},
		"splitHostPort(...): empty address": {
			address:      "",
			expectedHost: "",
			expectedPort: 0,
			expectedErr:  nil,
		},
	}
	for testName, testCase := range testCases {
		c.Logf("testing splitHostPort.%v", testName)

		hsot, port, err := splitHostPort(testCase.address)
		c.Assert(err, IsNil)
		c.Assert(hsot, Equals, testCase.expectedHost)
		c.Assert(port, Equals, testCase.expectedPort)
	}
}

func (s *TestSuite) TestExtractBackingImageAndDiskUUID(c *C) {
	fmt.Println("Testing ExtractBackingImageAndDiskUUID with various lvol name formats")

	type testCase struct {
		lvolName         string
		expectedBIName   string
		expectedDiskUUID string
		expectError      bool
	}
	testCases := map[string]testCase{
		"ExtractBackingImageAndDiskUUID(...): only disk with hyphens": {
			lvolName:         "bi-MyBackingImage-disk-12345-abcde",
			expectedBIName:   "MyBackingImage",
			expectedDiskUUID: "12345-abcde",
			expectError:      false,
		},
		"ExtractBackingImageAndDiskUUID(...): backing image name and disk with hyphens": {
			lvolName:         "bi-My-Backing-Image-disk-12345-abcde-xyz",
			expectedBIName:   "My-Backing-Image",
			expectedDiskUUID: "12345-abcde-xyz",
			expectError:      false,
		},
		"ExtractBackingImageAndDiskUUID(...): backing image name and disk without hyphens": {
			lvolName:         "bi-MyBackingImage-disk-12345",
			expectedBIName:   "MyBackingImage",
			expectedDiskUUID: "12345",
			expectError:      false,
		},
		"ExtractBackingImageAndDiskUUID(...):  doesn't start with bi- and doesn't contain -disk-": {
			lvolName:         "myWrongPattern",
			expectedBIName:   "",
			expectedDiskUUID: "",
			expectError:      true,
		},
		"ExtractBackingImageAndDiskUUID(...): doesn't have disk in the lvol name": {
			lvolName:         "bi-MyBackingImage-",
			expectedBIName:   "",
			expectedDiskUUID: "",
			expectError:      true,
		},
		"ExtractBackingImageAndDiskUUID(...): doesn't have backing image in the lvol name": {
			lvolName:         "-disk-123456-bi-abcdefg",
			expectedBIName:   "",
			expectedDiskUUID: "",
			expectError:      true,
		},
		"ExtractBackingImageAndDiskUUID(...): empty string": {
			lvolName:         "",
			expectedBIName:   "",
			expectedDiskUUID: "",
			expectError:      true,
		},
	}
	for testName, testCase := range testCases {
		c.Logf("testing ExtractBackingImageAndDiskUUID.%v", testName)
		// Call the function being tested
		biName, diskUUID, err := ExtractBackingImageAndDiskUUID(testCase.lvolName)

		if testCase.expectError {
			c.Assert(err, NotNil)
		} else {
			c.Assert(err, IsNil)
			c.Assert(testCase.expectedBIName, Equals, biName)
			c.Assert(testCase.expectedDiskUUID, Equals, diskUUID)
		}
	}
}

func (s *TestSuite) TestSetReplicaAdderInjectsRealFallback(c *C) {
	e := NewEngine("test-engine", "test-volume", types.FrontendEmpty, 1, NvmfTransportTCP, nil, defaultTestSnapshotMaxCount)

	firstMock := &MockReplicaAdder{}
	e.SetReplicaAdder(firstMock)

	firstFallback, ok := firstMock.Real.(*realReplicaAdder)
	c.Assert(ok, Equals, true)
	c.Assert(firstFallback.e, Equals, e)

	secondMock := &MockReplicaAdder{}
	e.SetReplicaAdder(secondMock)

	secondFallback, ok := secondMock.Real.(*realReplicaAdder)
	c.Assert(ok, Equals, true)
	c.Assert(secondFallback.e, Equals, e)
	c.Assert(secondMock.Real == firstMock, Equals, false)
}

func (s *TestSuite) TestAttachControllerRPCTimeout(c *C) {
	// The per-attach RPC ceiling scales with the connection's own timeouts, so
	// a short engine-create attach and a longer rebuild attach each get an
	// appropriate bound — and neither inherits the 24h long timeout.
	create := attachControllerRPCTimeout(replicaCtrlrLossTimeoutSec, replicaFastIOFailTimeoutSec)
	c.Assert(create, Equals, time.Duration(replicaCtrlrLossTimeoutSec+replicaFastIOFailTimeoutSec)*time.Second+attachRPCTimeoutSlack)

	rebuild := attachControllerRPCTimeout(rebuildCtrlrLossTimeoutSec, rebuildFastIOFailTimeoutSec)
	c.Assert(rebuild > create, Equals, true)
	// Both must be far below the 24h long timeout.
	c.Assert(rebuild < time.Hour, Equals, true)
}

// fakeAttachResolver implements attachControllerResolver for tests.
type fakeAttachResolver struct {
	bdevs         []spdktypes.BdevInfo
	getErr        error
	detachErr     error
	detachedNames []string
}

func (f *fakeAttachResolver) BdevGetBdevs(name string, timeout uint64) ([]spdktypes.BdevInfo, error) {
	return f.bdevs, f.getErr
}

func (f *fakeAttachResolver) BdevNvmeDetachController(name string) (bool, error) {
	f.detachedNames = append(f.detachedNames, name)
	return f.detachErr == nil, f.detachErr
}

func (s *TestSuite) TestResolveAttachAlreadyExists(c *C) {
	const ctrl = "pvc-x-r-abcdef01"

	// Namespace bdev present -> adopt the existing controller, do not detach.
	present := &fakeAttachResolver{bdevs: []spdktypes.BdevInfo{{}}}
	bdevName, adopted := resolveAttachAlreadyExists(present, ctrl)
	c.Assert(adopted, Equals, true)
	c.Assert(bdevName, Equals, ctrl+"n1")
	c.Assert(len(present.detachedNames), Equals, 0)

	// No namespace bdev (reconnecting to a dead target) -> detach once, no adopt.
	absent := &fakeAttachResolver{bdevs: nil}
	bdevName, adopted = resolveAttachAlreadyExists(absent, ctrl)
	c.Assert(adopted, Equals, false)
	c.Assert(bdevName, Equals, "")
	c.Assert(absent.detachedNames, DeepEquals, []string{ctrl})

	// BdevGetBdevs error is treated as "not present" -> detach, no adopt.
	getErr := &fakeAttachResolver{getErr: fmt.Errorf("rpc failed")}
	_, adopted = resolveAttachAlreadyExists(getErr, ctrl)
	c.Assert(adopted, Equals, false)
	c.Assert(getErr.detachedNames, DeepEquals, []string{ctrl})
}

func (s *TestSuite) TestConnectNVMfBdevAdrfamDetection(c *C) {
	testCases := []struct {
		address        string
		expectedIP     string
		expectedAdrfam spdktypes.NvmeAddressFamily
	}{
		{"[fd00::1]:20001", "fd00::1", spdktypes.NvmeAddressFamilyIPv6},
		{"10.0.0.1:20001", "10.0.0.1", spdktypes.NvmeAddressFamilyIPv4},
		{"[::1]:20001", "::1", spdktypes.NvmeAddressFamilyIPv6},
		{"192.168.1.100:8500", "192.168.1.100", spdktypes.NvmeAddressFamilyIPv4},
	}
	for _, tc := range testCases {
		ip, _, err := net.SplitHostPort(tc.address)
		c.Assert(err, IsNil)
		c.Assert(ip, Equals, tc.expectedIP)
		adrfam := spdkclient.DetectAddressFamily(ip)
		c.Assert(adrfam, Equals, tc.expectedAdrfam,
			Commentf("Wrong adrfam for %s", tc.address))
	}
}
