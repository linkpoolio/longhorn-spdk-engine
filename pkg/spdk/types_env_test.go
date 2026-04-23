package spdk

import (
	"os"

	. "gopkg.in/check.v1"
)

// setenv sets an environment variable for the duration of the surrounding
// test. gocheck's C lacks a Cleanup hook, so we use a defer returned as a
// closure that the caller must run via `defer`.
func setenv(c *C, name, value string) func() {
	orig, had := os.LookupEnv(name)
	c.Assert(os.Setenv(name, value), IsNil)
	return func() {
		if had {
			_ = os.Setenv(name, orig)
		} else {
			_ = os.Unsetenv(name)
		}
	}
}

func (s *TestSuite) TestEnvIntOrDefaultUsesDefaultWhenUnset(c *C) {
	const envName = "LONGHORN_TEST_ENV_UNSET_ABCDEF"
	c.Assert(os.Unsetenv(envName), IsNil)
	c.Assert(envIntOrDefault(envName, 42), Equals, 42)
}

func (s *TestSuite) TestEnvIntOrDefaultEmptyStringFallsBack(c *C) {
	const envName = "LONGHORN_TEST_ENV_EMPTY"
	defer setenv(c, envName, "")()
	c.Assert(envIntOrDefault(envName, 7), Equals, 7)
}

func (s *TestSuite) TestEnvIntOrDefaultParsesInteger(c *C) {
	const envName = "LONGHORN_TEST_ENV_INT"
	defer setenv(c, envName, "123")()
	c.Assert(envIntOrDefault(envName, 0), Equals, 123)
}

func (s *TestSuite) TestEnvIntOrDefaultTrimsWhitespace(c *C) {
	const envName = "LONGHORN_TEST_ENV_WHITESPACE"
	defer setenv(c, envName, "  55  ")()
	c.Assert(envIntOrDefault(envName, 0), Equals, 55)
}

func (s *TestSuite) TestEnvIntOrDefaultInvalidFallsBack(c *C) {
	const envName = "LONGHORN_TEST_ENV_BAD"
	defer setenv(c, envName, "not-a-number")()
	// Non-parseable values must fall back to default rather than propagating
	// a parse error — the init path has no error return.
	c.Assert(envIntOrDefault(envName, 99), Equals, 99)
}

func (s *TestSuite) TestEnvIntOrDefaultNegative(c *C) {
	const envName = "LONGHORN_TEST_ENV_NEG"
	defer setenv(c, envName, "-1")()
	c.Assert(envIntOrDefault(envName, 10), Equals, -1)
}

func (s *TestSuite) TestDefaultRaidDeltaBitmapEnabledDefaultsOn(c *C) {
	// Unset → default on. Guards against a future refactor that flips the
	// default off silently (would break incremental rebuild for every
	// existing cluster that doesn't set the env).
	c.Assert(os.Unsetenv("LONGHORN_V2_RAID_DELTA_BITMAP"), IsNil)
	c.Assert(defaultRaidDeltaBitmapEnabled(), Equals, true)
}

func (s *TestSuite) TestDefaultRaidDeltaBitmapEnabledHonorsFalseVariants(c *C) {
	for _, raw := range []string{"0", "false", "FALSE", "no", "off", "  false  "} {
		defer setenv(c, "LONGHORN_V2_RAID_DELTA_BITMAP", raw)()
		c.Assert(defaultRaidDeltaBitmapEnabled(), Equals, false, Commentf("raw=%q should disable", raw))
	}
}

func (s *TestSuite) TestDefaultRaidDeltaBitmapEnabledAnyOtherValueIsOn(c *C) {
	for _, raw := range []string{"1", "true", "yes", "on", "", "random-garbage"} {
		defer setenv(c, "LONGHORN_V2_RAID_DELTA_BITMAP", raw)()
		c.Assert(defaultRaidDeltaBitmapEnabled(), Equals, true, Commentf("raw=%q should enable", raw))
	}
}
