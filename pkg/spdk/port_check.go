package spdk

import (
	"fmt"
	"net"
	"strconv"
	"strings"

	"github.com/sirupsen/logrus"

	commonbitmap "github.com/longhorn/go-common-libs/bitmap"
)

// testPortBindFn checks that ip:port is bindable in this process's network
// namespace — the same namespace spdk_tgt binds its listeners in. On
// host-network instance managers the kernel shares that port space with every
// outbound connection on the node, and when the ephemeral source-port range
// overlaps the Longhorn port range, a freshly allocated port can already be
// held by some connection's source port. SPDK then fails the listener bind
// with EADDRINUSE, surfaced as an opaque "Invalid parameters" RPC error, and
// the instance create fails. Seam for tests.
var testPortBindFn = func(ip string, port int32) error {
	l, err := net.Listen("tcp4", net.JoinHostPort(ip, strconv.Itoa(int(port))))
	if err != nil {
		return err
	}
	return l.Close()
}

// allocateUsablePortRange allocates a port range from the allocator and
// verifies every port in it is actually bindable on ip before handing it out.
// A range containing a squatted port is left allocated — tainted — so it is
// never offered again for this IM's lifetime (squatters are transient kernel
// sockets; re-offering the port later would just re-roll the same dice while
// a create waits), and the next range is tried. The taint leaks a few ports
// per collision out of a ~10k range, which is deliberate and cheap.
func allocateUsablePortRange(allocator *commonbitmap.Bitmap, ip string, count int32, purpose string) (int32, int32, error) {
	const maxAttempts = 20
	for attempt := 0; attempt < maxAttempts; attempt++ {
		start, end, err := allocator.AllocateRange(count)
		if err != nil {
			return 0, 0, fmt.Errorf("failed to allocate port range for %s: %w", purpose, err)
		}
		port, bindErr := firstUnbindablePort(ip, start, end)
		if port == 0 {
			return start, end, nil
		}
		logrus.Warnf("Port preflight for %s: %s:%d is already in use in this network namespace (%v); tainting range [%d,%d] and retrying",
			purpose, ip, port, bindErr, start, end)
	}
	return 0, 0, fmt.Errorf("failed to find a bindable port range of size %d for %s after %d attempts", count, purpose, maxAttempts)
}

// firstUnbindablePort probes [start, end] and returns the first port that
// fails a test bind on ip along with its bind error, or 0 when every port in
// the range is bindable.
func firstUnbindablePort(ip string, start, end int32) (int32, error) {
	for p := start; p <= end; p++ {
		if bindErr := testPortBindFn(ip, p); bindErr != nil {
			return p, bindErr
		}
	}
	return 0, nil
}

// isListenerBindConflict reports whether an SPDK expose/add-listener error is
// the generic "Invalid parameters" RPC response. SPDK returns exactly that,
// with no further detail, when the listener's underlying TCP bind fails
// (EADDRINUSE from a kernel socket squatting the port). Callers construct the
// listener parameters themselves, so a genuinely invalid parameter is a
// programming error — treating the response as a port conflict and retrying
// on a fresh port is the only useful interpretation at runtime.
func isListenerBindConflict(err error) bool {
	return err != nil && strings.Contains(err.Error(), "Invalid parameters")
}
