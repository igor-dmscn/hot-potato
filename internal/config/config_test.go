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

func TestLoadDefaults(t *testing.T) {
	t.Parallel()

	cfg, err := Load(env(nil))
	if err != nil {
		t.Fatalf("Load with an empty environment: %v", err)
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

	cfg, err := Load(env(map[string]string{
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
			_, err := Load(env(tc.kv))
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

	_, err := Load(env(map[string]string{
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
