package spdk

import (
	"fmt"

	. "gopkg.in/check.v1"

	"github.com/sirupsen/logrus"

	"github.com/longhorn/go-spdk-helper/pkg/jsonrpc"
	spdktypes "github.com/longhorn/go-spdk-helper/pkg/spdk/types"

	safelog "github.com/longhorn/longhorn-spdk-engine/pkg/log"
)

// fakeRaidBdevManager is a programmable raidBdevManager for exercising
// Engine.ensureRaidBdev without a live spdk_tgt. BdevRaidCreate returns the next
// error from createErrs (nil once exhausted) so a test can script "first call
// EEXIST, second call succeeds".
type fakeRaidBdevManager struct {
	createErrs  []error
	createCalls int

	getResult []spdktypes.BdevInfo
	getErr    error
	getCalls  int

	deleteErr   error
	deleteCalls int
}

func (f *fakeRaidBdevManager) BdevRaidCreate(name string, raidLevel spdktypes.BdevRaidLevel, stripSizeKb uint32, baseBdevs []string, uuid string, deltaBitmap bool) (bool, error) {
	i := f.createCalls
	f.createCalls++
	var err error
	if i < len(f.createErrs) {
		err = f.createErrs[i]
	}
	return err == nil, err
}

func (f *fakeRaidBdevManager) BdevRaidGet(name string, timeout uint64) ([]spdktypes.BdevInfo, error) {
	f.getCalls++
	return f.getResult, f.getErr
}

func (f *fakeRaidBdevManager) BdevRaidDelete(name string) (bool, error) {
	f.deleteCalls++
	return f.deleteErr == nil, f.deleteErr
}

func newReconcileTestEngine() *Engine {
	return &Engine{
		Name: "test-engine",
		log:  safelog.NewSafeLogger(logrus.StandardLogger()),
	}
}

func rpcErr(code jsonrpc.RespErrorCode) error {
	return jsonrpc.JSONClientError{ErrorDetail: &jsonrpc.ResponseError{Code: code}}
}

func eexistErr() error       { return rpcErr(jsonrpc.RespErrorCodeNoFileExists) }
func noSuchDeviceErr() error { return rpcErr(jsonrpc.RespErrorCodeNoSuchDevice) }
func timeoutErr() error      { return rpcErr(jsonrpc.RespErrorCodeConnectionTimeout) }

func raidInfo(state string, baseNames ...string) spdktypes.BdevInfo {
	bases := make([]spdktypes.BaseBdev, 0, len(baseNames))
	for _, n := range baseNames {
		bases = append(bases, spdktypes.BaseBdev{Name: n})
	}
	return spdktypes.BdevInfo{
		DriverSpecific: &spdktypes.BdevDriverSpecific{
			Raid: &spdktypes.BdevRaidInfo{State: state, BaseBdevsList: bases},
		},
	}
}

// Happy path: the raid does not exist yet, BdevRaidCreate succeeds on the first
// call and we never inspect or delete anything.
func (s *TestSuite) TestEnsureRaidBdevCreateSucceeds(c *C) {
	f := &fakeRaidBdevManager{createErrs: []error{nil}}
	e := newReconcileTestEngine()
	c.Assert(e.ensureRaidBdev(f, []string{"r1", "r2"}), IsNil)
	c.Assert(f.createCalls, Equals, 1)
	c.Assert(f.getCalls, Equals, 0)
	c.Assert(f.deleteCalls, Equals, 0)
}

// A non-EEXIST create error is propagated verbatim and we do NOT start
// reconciling (no get/delete) — only EEXIST means "a raid already exists".
func (s *TestSuite) TestEnsureRaidBdevNonEEXISTErrorPropagates(c *C) {
	boom := fmt.Errorf("some unrelated rpc failure")
	f := &fakeRaidBdevManager{createErrs: []error{boom}}
	e := newReconcileTestEngine()
	err := e.ensureRaidBdev(f, []string{"r1"})
	c.Assert(err, Equals, boom)
	c.Assert(f.getCalls, Equals, 0)
	c.Assert(f.deleteCalls, Equals, 0)
}

// EEXIST + the existing raid is online with the same base bdev set (order
// independent) => adopt it: no delete, no second create.
func (s *TestSuite) TestEnsureRaidBdevAdoptsMatchingOnlineRaid(c *C) {
	f := &fakeRaidBdevManager{
		createErrs: []error{eexistErr()},
		getResult:  []spdktypes.BdevInfo{raidInfo("online", "r2", "r1")},
	}
	e := newReconcileTestEngine()
	c.Assert(e.ensureRaidBdev(f, []string{"r1", "r2"}), IsNil)
	c.Assert(f.createCalls, Equals, 1)
	c.Assert(f.getCalls, Equals, 1)
	c.Assert(f.deleteCalls, Equals, 0)
}

// EEXIST + the existing raid's base bdevs are the OLD controllers (mismatch)
// => delete the stale raid and rebuild over the current replicas.
func (s *TestSuite) TestEnsureRaidBdevRebuildsMismatchedRaid(c *C) {
	f := &fakeRaidBdevManager{
		createErrs: []error{eexistErr(), nil},
		getResult:  []spdktypes.BdevInfo{raidInfo("online", "old-r1", "old-r2")},
	}
	e := newReconcileTestEngine()
	c.Assert(e.ensureRaidBdev(f, []string{"r1", "r2"}), IsNil)
	c.Assert(f.deleteCalls, Equals, 1)
	c.Assert(f.createCalls, Equals, 2)
}

// EEXIST + the existing raid is not online (e.g. stuck "configuring") => rebuild
// even though the base bdev names happen to match.
func (s *TestSuite) TestEnsureRaidBdevRebuildsOfflineRaid(c *C) {
	f := &fakeRaidBdevManager{
		createErrs: []error{eexistErr(), nil},
		getResult:  []spdktypes.BdevInfo{raidInfo("configuring", "r1", "r2")},
	}
	e := newReconcileTestEngine()
	c.Assert(e.ensureRaidBdev(f, []string{"r1", "r2"}), IsNil)
	c.Assert(f.deleteCalls, Equals, 1)
	c.Assert(f.createCalls, Equals, 2)
}

// EEXIST + the stale raid cannot be deleted yet (delete returns -110 because its
// base controller is still stuck "deleting") => surface the error so the next
// engine reconcile retries; do NOT recreate over a still-present raid.
func (s *TestSuite) TestEnsureRaidBdevDeleteStillStuckReturnsErrorForRetry(c *C) {
	f := &fakeRaidBdevManager{
		createErrs: []error{eexistErr()},
		getResult:  []spdktypes.BdevInfo{raidInfo("online", "old-r1")},
		deleteErr:  timeoutErr(),
	}
	e := newReconcileTestEngine()
	err := e.ensureRaidBdev(f, []string{"r1"})
	c.Assert(err, NotNil)
	c.Assert(f.deleteCalls, Equals, 1)
	c.Assert(f.createCalls, Equals, 1)
}

// EEXIST + delete races to NoSuchDevice (someone else removed it) => tolerate
// and proceed to rebuild.
func (s *TestSuite) TestEnsureRaidBdevDeleteNoSuchDeviceProceedsToRebuild(c *C) {
	f := &fakeRaidBdevManager{
		createErrs: []error{eexistErr(), nil},
		getResult:  []spdktypes.BdevInfo{raidInfo("online", "old-r1")},
		deleteErr:  noSuchDeviceErr(),
	}
	e := newReconcileTestEngine()
	c.Assert(e.ensureRaidBdev(f, []string{"r1"}), IsNil)
	c.Assert(f.deleteCalls, Equals, 1)
	c.Assert(f.createCalls, Equals, 2)
}

// EEXIST + the raid raced away between the create and the get (get returns
// NoSuchDevice) => just retry the create, no delete.
func (s *TestSuite) TestEnsureRaidBdevGetNoSuchDeviceRetriesCreate(c *C) {
	f := &fakeRaidBdevManager{
		createErrs: []error{eexistErr(), nil},
		getErr:     noSuchDeviceErr(),
	}
	e := newReconcileTestEngine()
	c.Assert(e.ensureRaidBdev(f, []string{"r1"}), IsNil)
	c.Assert(f.getCalls, Equals, 1)
	c.Assert(f.deleteCalls, Equals, 0)
	c.Assert(f.createCalls, Equals, 2)
}

// EEXIST + an unexpected error inspecting the raid => propagate, don't blindly
// delete a raid we couldn't inspect.
func (s *TestSuite) TestEnsureRaidBdevGetErrorPropagates(c *C) {
	f := &fakeRaidBdevManager{
		createErrs: []error{eexistErr()},
		getErr:     fmt.Errorf("rpc socket broke"),
	}
	e := newReconcileTestEngine()
	c.Assert(e.ensureRaidBdev(f, []string{"r1"}), NotNil)
	c.Assert(f.deleteCalls, Equals, 0)
	c.Assert(f.createCalls, Equals, 1)
}

func (s *TestSuite) TestRaidBaseBdevsMatch(c *C) {
	c.Assert(raidBaseBdevsMatch(nil, []string{"r1"}), Equals, false)
	c.Assert(raidBaseBdevsMatch(&spdktypes.BdevInfo{}, []string{"r1"}), Equals, false)

	off := raidInfo("configuring", "r1")
	c.Assert(raidBaseBdevsMatch(&off, []string{"r1"}), Equals, false)

	match := raidInfo("online", "r2", "r1")
	c.Assert(raidBaseBdevsMatch(&match, []string{"r1", "r2"}), Equals, true)

	extra := raidInfo("online", "r1", "r2", "r3")
	c.Assert(raidBaseBdevsMatch(&extra, []string{"r1", "r2"}), Equals, false)

	diff := raidInfo("online", "r1", "rX")
	c.Assert(raidBaseBdevsMatch(&diff, []string{"r1", "r2"}), Equals, false)

	// State comparison is case-insensitive.
	ci := raidInfo("Online", "r1")
	c.Assert(raidBaseBdevsMatch(&ci, []string{"r1"}), Equals, true)
}

func (s *TestSuite) TestEqualStringSet(c *C) {
	c.Assert(equalStringSet(nil, nil), Equals, true)
	c.Assert(equalStringSet([]string{"a"}, []string{"a"}), Equals, true)
	c.Assert(equalStringSet([]string{"a", "b"}, []string{"b", "a"}), Equals, true)
	c.Assert(equalStringSet([]string{"a"}, []string{"a", "b"}), Equals, false)
	// Multiset semantics: duplicate counts must match.
	c.Assert(equalStringSet([]string{"a", "a"}, []string{"a", "b"}), Equals, false)
	c.Assert(equalStringSet([]string{"a", "a"}, []string{"a", "a"}), Equals, true)
}
