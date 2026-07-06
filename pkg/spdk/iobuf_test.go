package spdk

import (
	"testing"
)

// The derived pools must cover the demand that starved the fleet on
// 2026-07-06: transports' shared buffers moved into the shared iobuf pool on
// SPDK v26.05, plus capped per-poll-group caches, plus channel-cache
// allowances. Cases pin the two production node shapes that failed.
func TestIobufPoolCounts(t *testing.T) {
	tcpShared := uint64(nvmfTcpOpts.NumSharedBuffers)
	rdmaShared := uint64(nvmfRdmaOpts.NumSharedBuffers)

	cases := []struct {
		name     string
		rdma     bool
		reactors int
	}{
		{"tcp consumer node", false, 4},
		{"rdma storage node 16 reactors (ma3-worker-9)", true, 16},
		{"rdma node few reactors (ma4-worker-1)", true, 4},
		{"degenerate reactor count", true, 0},
	}
	for _, tc := range cases {
		small, large := iobufPoolCounts(tc.rdma, tc.reactors)

		minLarge := iobufBaseLargePoolCount + tcpShared
		if tc.rdma {
			minLarge += rdmaShared
		}
		if large < minLarge {
			t.Errorf("%s: large=%d does not cover base+shared demand %d", tc.name, large, minLarge)
		}
		if small < iobufBaseSmallPoolCount {
			t.Errorf("%s: small=%d below SPDK baseline %d", tc.name, small, iobufBaseSmallPoolCount)
		}

		// The pools must fit the smallest production hugepage budget for the
		// node class (8GiB on RDMA nodes, 2GiB floor on TCP nodes) under the
		// budget fraction.
		budget := uint64(2) << 30
		if tc.rdma {
			budget = 8 << 30
		}
		if need := iobufPoolBytes(small, large); float64(need) > float64(budget)*iobufBudgetMaxFraction {
			t.Errorf("%s: pools need %dMiB, over %d%% of %dMiB budget",
				tc.name, need>>20, int(iobufBudgetMaxFraction*100), budget>>20)
		}
	}
}

// Demand must not scale with the pool (the v26.05 default-cache trap): the
// derivation depends only on configuration, so two consecutive computations
// are identical, and the transport opts must carry explicit cache caps.
func TestIobufTransportCacheCapsExplicit(t *testing.T) {
	if nvmfTcpOpts.IobufLargeCacheSize == 0 || nvmfTcpOpts.IobufSmallCacheSize == 0 {
		t.Fatal("TCP transport opts must cap iobuf caches explicitly")
	}
	if nvmfRdmaOpts.IobufLargeCacheSize == 0 || nvmfRdmaOpts.IobufSmallCacheSize == 0 {
		t.Fatal("RDMA transport opts must cap iobuf caches explicitly")
	}
	if nvmfTcpOpts.BufCacheSize != 0 || nvmfRdmaOpts.BufCacheSize != 0 {
		t.Fatal("buf_cache_size must not be sent alongside iobuf_small_cache_size (shared C-union decode slot)")
	}
}
