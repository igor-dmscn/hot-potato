package config

import (
	"log/slog"
	"strings"
	"testing"
	"time"
)

// env builds a lookup func from a map. Absent keys are absent, not empty.
func env(kv map[string]string) func(string) (string, bool) {
	return func(k string) (string, bool) {
		v, ok := kv[k]
		return v, ok
	}
}

// required is the smallest environment that loads. Everything else has a default.
var required = map[string]string{"HP_DATABASE_URL": "postgres://u:p@localhost:5432/hp"}

func with(kv map[string]string) func(string) (string, bool) {
	merged := map[string]string{}
	for k, v := range required {
		merged[k] = v
	}
	for k, v := range kv {
		merged[k] = v
	}
	return env(merged)
}

func TestLoadDefaults(t *testing.T) {
	t.Parallel()

	cfg, err := Load(with(nil))
	if err != nil {
		t.Fatalf("Load with only the required variables: %v", err)
	}
	if cfg.Addr != ":8080" {
		t.Errorf("Addr = %q, want :8080", cfg.Addr)
	}
	if cfg.InstanceID != "inst-local" {
		t.Errorf("InstanceID = %q, want inst-local", cfg.InstanceID)
	}
	if cfg.LogLevel != slog.LevelInfo {
		t.Errorf("LogLevel = %v, want info", cfg.LogLevel)
	}
	if cfg.ShutdownGrace != 15*time.Second {
		t.Errorf("ShutdownGrace = %v, want 15s", cfg.ShutdownGrace)
	}
}

func TestLoadOverrides(t *testing.T) {
	t.Parallel()

	cfg, err := Load(with(map[string]string{
		"HP_ADDR":           "127.0.0.1:9000",
		"HP_EXTERNAL_URL":   "https://hp.example.com/",
		"HP_INSTANCE_ID":    "inst-b",
		"HP_LOG_LEVEL":      "debug",
		"HP_SHUTDOWN_GRACE": "3s",
	}))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Addr != "127.0.0.1:9000" {
		t.Errorf("Addr = %q", cfg.Addr)
	}
	if cfg.ExternalURL != "https://hp.example.com" {
		t.Errorf("ExternalURL = %q, want the trailing slash trimmed", cfg.ExternalURL)
	}
	if cfg.LogLevel != slog.LevelDebug {
		t.Errorf("LogLevel = %v, want debug", cfg.LogLevel)
	}
	if cfg.ShutdownGrace != 3*time.Second {
		t.Errorf("ShutdownGrace = %v, want 3s", cfg.ShutdownGrace)
	}
}

func TestLoadRejects(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		kv   map[string]string
		want string
	}{
		"bad duration":           {map[string]string{"HP_SHUTDOWN_GRACE": "abc"}, `HP_SHUTDOWN_GRACE="abc" is not a duration`},
		"zero duration":          {map[string]string{"HP_SHUTDOWN_GRACE": "0s"}, `must be positive`},
		"empty is not a default": {map[string]string{"HP_ADDR": ""}, `HP_ADDR="" is set but empty`},
		"bad level":              {map[string]string{"HP_LOG_LEVEL": "chatty"}, `is not a log level`},
		"relative url":           {map[string]string{"HP_EXTERNAL_URL": "/hp"}, `is not an absolute URL`},
		"dotted instance":        {map[string]string{"HP_INSTANCE_ID": "inst.b"}, `must not contain a dot`},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			_, err := Load(with(tc.kv))
			if err == nil {
				t.Fatalf("Load(%v) succeeded, want an error", tc.kv)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error = %q, want it to contain %q", err, tc.want)
			}
		})
	}
}

// Fail fast, but report every problem at once: one restart, not three.
func TestLoadReportsEveryProblemInOnePass(t *testing.T) {
	t.Parallel()

	_, err := Load(with(map[string]string{
		"HP_SHUTDOWN_GRACE": "abc",
		"HP_LOG_LEVEL":      "chatty",
		"HP_INSTANCE_ID":    "inst.b",
	}))
	if err == nil {
		t.Fatal("Load succeeded with three broken values")
	}
	for _, want := range []string{"HP_SHUTDOWN_GRACE", "HP_LOG_LEVEL", "HP_INSTANCE_ID"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %s", err, want)
		}
	}
}

// A value with no safe default must be supplied, whatever the environment. There
// is no HP_ENV=prod to forget: a profile that switches these on fails open.
func TestLoadRequiresWhatHasNoSafeDefault(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		kv   map[string]string
		want string
	}{
		"database always": {
			nil,
			"HP_DATABASE_URL is required",
		},
		// Standalone keeps its defaults: inst-local is only dangerous once a
		// second instance can hear about it.
		"instance id once distributed": {
			map[string]string{"HP_REDIS_URL": "redis://localhost:6379/0"},
			"HP_INSTANCE_ID is required",
		},
		"external url once distributed": {
			map[string]string{"HP_REDIS_URL": "redis://localhost:6379/0"},
			"HP_EXTERNAL_URL is required",
		},
		"nats url when the bus is nats": {
			map[string]string{"HP_BUS": "nats"},
			"HP_NATS_URL is required",
		},
		"kafka brokers when the bus is kafka": {
			map[string]string{"HP_BUS": "kafka"},
			"HP_KAFKA_BROKERS is required",
		},
		"redis url when the bus is redis": {
			map[string]string{"HP_BUS": "redis"},
			"HP_REDIS_URL is required",
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			// Deliberately not with(): the point is what is missing.
			kv := map[string]string{}
			if tc.want != "HP_DATABASE_URL is required" {
				kv["HP_DATABASE_URL"] = required["HP_DATABASE_URL"]
			}
			for k, v := range tc.kv {
				kv[k] = v
			}
			_, err := Load(env(kv))
			if err == nil {
				t.Fatalf("Load(%v) succeeded, want %q", kv, tc.want)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error = %q, want it to contain %q", err, tc.want)
			}
		})
	}
}

// Standalone is the zero-configuration case and must stay that way.
func TestLoadStandaloneNeedsNoInstanceIdentity(t *testing.T) {
	t.Parallel()

	cfg, err := Load(with(nil))
	if err != nil {
		t.Fatalf("standalone Load: %v", err)
	}
	if cfg.InstanceID != "inst-local" || cfg.ExternalURL != "http://localhost:8080" {
		t.Errorf("InstanceID = %q, ExternalURL = %q, want the standalone defaults",
			cfg.InstanceID, cfg.ExternalURL)
	}
}
