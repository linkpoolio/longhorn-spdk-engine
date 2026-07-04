package spdk

import (
	"fmt"
	"time"

	. "gopkg.in/check.v1"

	"github.com/longhorn/longhorn-spdk-engine/pkg/types"
)

func (s *TestSuite) TestShallowCopyProgressTrackerStallDetection(c *C) {
	fmt.Println("Testing shallowCopyProgressTracker stall detection")

	t := &shallowCopyProgressTracker{}
	base := time.Now()

	// First in-progress observation only arms the tracker.
	stalled, dur := t.Observe("snap1", types.ProgressStateInProgress, 100, base)
	c.Assert(stalled, Equals, false)
	c.Assert(dur, Equals, time.Duration(0))

	// Same count within the threshold: not stalled yet, duration reported.
	stalled, dur = t.Observe("snap1", types.ProgressStateInProgress, 100, base.Add(MaxShallowCopyStallTime/2))
	c.Assert(stalled, Equals, false)
	c.Assert(dur, Equals, MaxShallowCopyStallTime/2)

	// Same count past the threshold: stalled.
	stalled, dur = t.Observe("snap1", types.ProgressStateInProgress, 100, base.Add(MaxShallowCopyStallTime+time.Second))
	c.Assert(stalled, Equals, true)
	c.Assert(dur, Equals, MaxShallowCopyStallTime+time.Second)
}

func (s *TestSuite) TestShallowCopyProgressTrackerProgressResetsTimer(c *C) {
	fmt.Println("Testing shallowCopyProgressTracker timer reset on progress")

	t := &shallowCopyProgressTracker{}
	base := time.Now()

	t.Observe("snap1", types.ProgressStateInProgress, 100, base)

	// The counter advances just before the threshold: timer restarts.
	stalled, dur := t.Observe("snap1", types.ProgressStateInProgress, 101, base.Add(MaxShallowCopyStallTime))
	c.Assert(stalled, Equals, false)
	c.Assert(dur, Equals, time.Duration(0))

	// Well past the original deadline but within the new one: not stalled.
	stalled, _ = t.Observe("snap1", types.ProgressStateInProgress, 101, base.Add(MaxShallowCopyStallTime+MaxShallowCopyStallTime/2))
	c.Assert(stalled, Equals, false)

	// Frozen past the new deadline: stalled.
	stalled, dur = t.Observe("snap1", types.ProgressStateInProgress, 101, base.Add(2*MaxShallowCopyStallTime+time.Second))
	c.Assert(stalled, Equals, true)
	c.Assert(dur, Equals, MaxShallowCopyStallTime+time.Second)
}

func (s *TestSuite) TestShallowCopyProgressTrackerNonInProgressNeverStalls(c *C) {
	fmt.Println("Testing shallowCopyProgressTracker ignores non-in-progress states")

	base := time.Now()
	for _, state := range []string{types.ProgressStateStarting, types.ProgressStateComplete, types.ProgressStateError, ""} {
		t := &shallowCopyProgressTracker{}
		t.Observe("snap1", state, 100, base)
		stalled, dur := t.Observe("snap1", state, 100, base.Add(10*MaxShallowCopyStallTime))
		c.Assert(stalled, Equals, false)
		c.Assert(dur, Equals, time.Duration(0))
	}

	// A copy that stalls and then completes must never report stalled again.
	t := &shallowCopyProgressTracker{}
	t.Observe("snap1", types.ProgressStateInProgress, 100, base)
	stalled, _ := t.Observe("snap1", types.ProgressStateInProgress, 100, base.Add(MaxShallowCopyStallTime+time.Second))
	c.Assert(stalled, Equals, true)
	stalled, _ = t.Observe("snap1", types.ProgressStateComplete, 100, base.Add(MaxShallowCopyStallTime+2*time.Second))
	c.Assert(stalled, Equals, false)

	// Non-in-progress re-arms the tracker: the next in-progress poll starts a
	// fresh window instead of inheriting the stale timestamp.
	stalled, dur := t.Observe("snap2", types.ProgressStateInProgress, 100, base.Add(2*MaxShallowCopyStallTime))
	c.Assert(stalled, Equals, false)
	c.Assert(dur, Equals, time.Duration(0))
}

func (s *TestSuite) TestShallowCopyProgressTrackerSnapshotSwitchAndReset(c *C) {
	fmt.Println("Testing shallowCopyProgressTracker snapshot switch and Reset")

	t := &shallowCopyProgressTracker{}
	base := time.Now()

	t.Observe("snap1", types.ProgressStateInProgress, 100, base)

	// A new snapshot with the same handled count re-baselines the timer.
	stalled, dur := t.Observe("snap2", types.ProgressStateInProgress, 100, base.Add(MaxShallowCopyStallTime+time.Second))
	c.Assert(stalled, Equals, false)
	c.Assert(dur, Equals, time.Duration(0))

	// Reset drops all state, e.g. when a retried rebuild reuses the dst cache.
	t.Observe("snap2", types.ProgressStateInProgress, 100, base.Add(MaxShallowCopyStallTime+2*time.Second))
	t.Reset()
	stalled, dur = t.Observe("snap2", types.ProgressStateInProgress, 100, base.Add(3*MaxShallowCopyStallTime))
	c.Assert(stalled, Equals, false)
	c.Assert(dur, Equals, time.Duration(0))
}
