package spdk

import (
	"errors"
	"testing"
)

func TestIsFrameworkAlreadyInitialized(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil error", nil, false},
		// SPDK's actual INVALID_STATE response to framework_start_init in RUNTIME.
		{"runtime already-initialized message", errors.New("Method may only be called before framework is initialized. Use --wait-for-rpc command line parameter and then issue this RPC before the framework_start_init RPC."), true},
		// The STARTUP-state message for RUNTIME-only RPCs ("after") must NOT match,
		// so we never mask a genuine "framework not yet up" error.
		{"startup not-yet-initialized message", errors.New("Method may only be called after framework is initialized using framework_start_init RPC."), false},
		{"unrelated error", errors.New("connection refused"), false},
	}
	for _, tc := range cases {
		if got := isFrameworkAlreadyInitialized(tc.err); got != tc.want {
			t.Errorf("isFrameworkAlreadyInitialized(%v) = %v, want %v", tc.err, got, tc.want)
		}
	}
}
