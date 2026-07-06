package spdk

import (
	"errors"
	"fmt"
	"os"
	"runtime"
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

// These cover the transport-opts helpers and the cntlid allocation scheme.
// They are pure / env-driven, so they run as standard go tests alongside the
// gocheck suite (Test in util_test.go).

func TestEnvIntOrDefault(t *testing.T) {
	const key = "LONGHORN_V2_TEST_ENV_INT"
	cases := []struct {
		name string
		set  bool
		val  string
		def  int
		want int
	}{
		{"unset returns default", false, "", 7, 7},
		{"empty returns default", true, "", 7, 7},
		{"valid integer parsed", true, "42", 7, 42},
		{"surrounding whitespace trimmed", true, "  13 ", 7, 13},
		{"negative integer parsed", true, "-5", 7, -5},
		{"zero parsed", true, "0", 7, 0},
		{"unparseable returns default", true, "notanint", 7, 7},
		{"trailing garbage returns default", true, "12x", 7, 7},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.set {
				t.Setenv(key, tc.val)
			} else {
				os.Unsetenv(key)
			}
			if got := envIntOrDefault(key, tc.def); got != tc.want {
				t.Fatalf("envIntOrDefault(%q, %d) = %d, want %d", tc.val, tc.def, got, tc.want)
			}
		})
	}
}

func TestSPDKCoreCount(t *testing.T) {
	cpus := runtime.NumCPU()
	cases := []struct {
		name string
		set  bool
		mask string
		want int
	}{
		{"unset falls back to NumCPU", false, "", cpus},
		{"single bit", true, "0x1", 1},
		{"two bits 0x3", true, "0x3", 2},
		{"0xf is four cores", true, "0xf", 4},
		{"no prefix ff is eight cores", true, "ff", 8},
		{"uppercase 0X prefix", true, "0X3", 2},
		{"whitespace trimmed", true, "  0x7 ", 3},
		{"zero mask falls back to NumCPU", true, "0x0", cpus},
		{"unparseable falls back to NumCPU", true, "zzz", cpus},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.set {
				t.Setenv("LONGHORN_V2_SPDK_CPUMASK", tc.mask)
			} else {
				os.Unsetenv("LONGHORN_V2_SPDK_CPUMASK")
			}
			if got := spdkCoreCount(); got != tc.want {
				t.Fatalf("spdkCoreCount() with mask %q = %d, want %d", tc.mask, got, tc.want)
			}
		})
	}
}

func TestAccelMlx5NumRequests(t *testing.T) {
	// A 2-core cpumask yields cores * mkeys-per-core.
	t.Setenv("LONGHORN_V2_SPDK_CPUMASK", "0x3")
	want := uint32(2) * accelMlx5MkeysPerCore
	if got := accelMlx5NumRequests(); got != want {
		t.Fatalf("accelMlx5NumRequests() = %d, want %d (2 cores * %d)", got, want, accelMlx5MkeysPerCore)
	}
}

func TestGetEngineCntlid(t *testing.T) {
	cases := []struct {
		engine string
		want   uint16
	}{
		{"vol-e-0", 1},
		{"vol-e-1", 2},
		{"vol-e-5", 6},
		{"my-vol-e-9", 10},
		{"noordinal", 1}, // no parseable trailing ordinal -> fallback
		{"vol-e-abc", 1}, // non-numeric ordinal -> fallback
		{"", 1},          // empty -> fallback
	}
	for _, tc := range cases {
		if got := getEngineCntlid(tc.engine); got != tc.want {
			t.Errorf("getEngineCntlid(%q) = %d, want %d", tc.engine, got, tc.want)
		}
	}
}

func TestGetEngineCntlidRange(t *testing.T) {
	cases := []struct {
		engine string
		lo, hi uint16
	}{
		{"vol-e-0", 1001, 17000},  // cntlid 1 -> slot 0
		{"vol-e-1", 17001, 33000}, // cntlid 2 -> slot 1
		{"vol-e-2", 33001, 49000}, // cntlid 3 -> slot 2
		{"vol-e-3", 49001, 65000}, // cntlid 4 -> slot 3
		{"vol-e-4", 1001, 17000},  // cntlid 5 -> slot 0 (wraps)
	}
	for _, tc := range cases {
		lo, hi := getEngineCntlidRange(tc.engine)
		if lo != tc.lo || hi != tc.hi {
			t.Errorf("getEngineCntlidRange(%q) = (%d, %d), want (%d, %d)", tc.engine, lo, hi, tc.lo, tc.hi)
		}
	}

	// The invariant that matters operationally: the two consecutive-ordinal
	// engines that briefly share an NQN during a live migration / upgrade must
	// get disjoint cntlid windows, and every window must stay inside the SPDK
	// valid cntlid space (<= 0xffef).
	const maxValidCntlid uint16 = 0xffef
	for ord := 0; ord < 16; ord++ {
		a := fmt.Sprintf("vol-e-%d", ord)
		b := fmt.Sprintf("vol-e-%d", ord+1)
		alo, ahi := getEngineCntlidRange(a)
		blo, bhi := getEngineCntlidRange(b)

		if ahi > maxValidCntlid || bhi > maxValidCntlid {
			t.Errorf("window exceeds SPDK cntlid space: %s=(%d,%d) %s=(%d,%d) max=%d", a, alo, ahi, b, blo, bhi, maxValidCntlid)
		}
		// Half-open overlap test on inclusive ranges.
		if alo <= bhi && blo <= ahi {
			t.Errorf("consecutive ordinals share a cntlid window: %s=(%d,%d) overlaps %s=(%d,%d)", a, alo, ahi, b, blo, bhi)
		}
	}
}
