package spdk

import (
	"time"

	"github.com/longhorn/longhorn-spdk-engine/pkg/types"
)

// shallowCopyProgressTracker watches the handled-cluster counter of a snapshot
// shallow copy and reports when a copy that claims to be in progress has stopped
// advancing. During a storage-node brownout the SPDK copy op can freeze while
// every status RPC keeps succeeding with the same copied_clusters value, so the
// RPC-error-based aborts never fire and the rebuild hangs until
// MaxShallowCopyWaitTime (72h). Time only accumulates while the reported state
// is in-progress with an unchanged cluster count for the same snapshot;
// starting, complete, and error states never count as stalled.
type shallowCopyProgressTracker struct {
	tracking       bool
	snapshotName   string
	lastHandled    uint64
	lastProgressAt time.Time
}

func (t *shallowCopyProgressTracker) Reset() {
	*t = shallowCopyProgressTracker{}
}

// Observe records one status poll. It returns stalled=true once the copy has
// reported the same handled-cluster count for the same snapshot for longer than
// MaxShallowCopyStallTime, along with how long the counter has been frozen.
// Any counter movement, snapshot switch, or non-in-progress state re-arms the
// tracker.
func (t *shallowCopyProgressTracker) Observe(snapshotName, state string, handledClusters uint64, now time.Time) (stalled bool, stallDuration time.Duration) {
	if state != types.ProgressStateInProgress {
		t.tracking = false
		return false, 0
	}
	if !t.tracking || t.snapshotName != snapshotName || t.lastHandled != handledClusters {
		t.tracking = true
		t.snapshotName = snapshotName
		t.lastHandled = handledClusters
		t.lastProgressAt = now
		return false, 0
	}
	stallDuration = now.Sub(t.lastProgressAt)
	return stallDuration > MaxShallowCopyStallTime, stallDuration
}
