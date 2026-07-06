package spdk

import (
	"testing"
)

// Budget-first sizing: the pools take 50% of the node's SPDK hugepage
// allocation by default, split 7:1 large:small by bytes, floored at the SPDK
// baselines. Cases pin the production node shapes.
func TestIobufPoolCountsBudget(t *testing.T) {
	cases := []struct {
		name      string
		rdma      bool
		reactors  int
		budgetMiB uint64
	}{
		{"tcp consumer node, 2GiB default", false, 4, 2048},
		{"rdma storage node, 16GiB (ma3-worker-9)", true, 16, 16384},
		{"rdma node, 8GiB (ma3-worker-11)", true, 16, 8192},
		{"rdma node, few reactors (ma4-worker-1)", true, 4, 16384},
	}
	for _, tc := range cases {
		budget := tc.budgetMiB << 20
		small, large := iobufPoolCounts(tc.rdma, tc.reactors, budget)

		need := iobufPoolBytes(small, large)
		lo, hi := budget*45/100, budget*55/100
		if need < lo || need > hi {
			t.Errorf("%s: pools use %dMiB, want ~50%% of %dMiB", tc.name, need>>20, tc.budgetMiB)
		}
		if large < iobufBaseLargePoolCount || small < iobufBaseSmallPoolCount {
			t.Errorf("%s: pools below SPDK baselines (small=%d large=%d)", tc.name, small, large)
		}
		// The large pool must comfortably cover the init-time cache
		// population that starved the fleet: capped transport caches x
		// reactors, both transports.
		caches := (uint64(nvmfTcpOpts.IobufLargeCacheSize) + uint64(nvmfRdmaOpts.IobufLargeCacheSize)) * uint64(tc.reactors)
		if large < iobufBaseLargePoolCount+caches {
			t.Errorf("%s: large=%d cannot cover base+capped caches %d", tc.name, large, iobufBaseLargePoolCount+caches)
		}
	}
}

// With no discoverable budget, the configured-demand fallback covers the
// SPDK baselines plus both transports' shared buffers.
func TestIobufPoolCountsFallback(t *testing.T) {
	small, large := iobufPoolCounts(true, 16, 0)
	minLarge := iobufBaseLargePoolCount + uint64(nvmfTcpOpts.NumSharedBuffers) + uint64(nvmfRdmaOpts.NumSharedBuffers)
	if large < minLarge {
		t.Errorf("fallback large=%d does not cover base+shared demand %d", large, minLarge)
	}
	if small < iobufBaseSmallPoolCount {
		t.Errorf("fallback small=%d below SPDK baseline", small)
	}
}

// Absolute overrides trump the budget derivation.
func TestIobufPoolCountsOverride(t *testing.T) {
	t.Setenv("LONGHORN_V2_IOBUF_LARGE_POOL_COUNT", "12345")
	t.Setenv("LONGHORN_V2_IOBUF_SMALL_POOL_COUNT", "23456")
	small, large := iobufPoolCounts(true, 16, 8<<30)
	if large != 12345 || small != 23456 {
		t.Errorf("overrides not honored: small=%d large=%d", small, large)
	}
}

// The transports must carry explicit iobuf cache caps: the v26.05 default
// cache is pool/(2*poll_groups) per transport, which consumes the entire
// pool at any size. buf_cache_size shares a C-union decode slot with
// iobuf_small_cache_size and must never be sent alongside it.
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
