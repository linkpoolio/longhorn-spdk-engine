package spdk

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	. "gopkg.in/check.v1"
)

// Every NVMe-oF expose in this package must be transport-aware: the plain
// TCP-only StartExposeBdev silently contradicts the dual-listener addresses
// this package advertises (headLvolTransportAddresses) and the transports the
// dialers use, which breaks rebuilds/clones/re-exposes on RDMA nodes. The
// 1.12 rebase reintroduced plain calls at six sites and shipped; this test
// makes that class of regression fail CI instead.
func (s *TestSuite) TestNoPlainStartExposeBdevCallSites(c *C) {
	fmt.Println("Testing that all expose call sites are transport-aware")

	// Matches ".StartExposeBdev(" but not ".StartExposeBdevWithTransport("
	// or other suffixed variants.
	plainExpose := regexp.MustCompile(`\.StartExposeBdev\(`)

	entries, err := filepath.Glob("*.go")
	c.Assert(err, IsNil)
	c.Assert(len(entries) > 0, Equals, true)

	var offenders []string
	for _, path := range entries {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		data, err := os.ReadFile(path)
		c.Assert(err, IsNil)
		for i, line := range strings.Split(string(data), "\n") {
			if plainExpose.MatchString(line) {
				offenders = append(offenders, fmt.Sprintf("%s:%d: %s", path, i+1, strings.TrimSpace(line)))
			}
		}
	}

	c.Assert(offenders, HasLen, 0, Commentf(
		"plain StartExposeBdev creates a TCP-only listener that contradicts the advertised/dialed transports on RDMA nodes; use StartExposeBdevWithTransport(r.transport()) + addTCPFallbackListener:\n%s",
		strings.Join(offenders, "\n")))
}
