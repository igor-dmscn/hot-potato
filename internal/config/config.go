// Package config parses the process environment into one immutable, typed
// value at startup.
//
// Only cmd/ imports this package. Every other package declares its own Options
// struct and main fills it in, so that no package needs the whole
// application's configuration shape to be readable or testable. That is the
// difference between configuration being an input and configuration being an
// ambient dependency.
package config

import (
	"fmt"
	"log/slog"
	"net/url"
	"strings"
	"time"
)

// Config is the resolved configuration. Load either returns a fully populated
// Config or an error naming everything wrong with the environment — one
// restart, not three.
type Config struct {
	Addr          string        // HP_ADDR, default ":8080"
	ExternalURL   string        // HP_EXTERNAL_URL — how other instances reach this one
	InstanceID    string        // HP_INSTANCE_ID — the prefix of every Transfer ID it owns
	LogLevel      slog.Level    // HP_LOG_LEVEL
	ShutdownGrace time.Duration // HP_SHUTDOWN_GRACE
}

// Load reads configuration through lookup, which has os.LookupEnv's signature.
// Injecting it instead of reading the process environment keeps these tests
// pure and parallel: t.Setenv forbids t.Parallel, and a global environment is
// shared mutable state between tests.
func Load(lookup func(string) (string, bool)) (Config, error) {
	l := loader{lookup: lookup}
	cfg := Config{
		Addr:          l.str("HP_ADDR", ":8080"),
		ExternalURL:   l.url("HP_EXTERNAL_URL", "http://localhost:8080"),
		InstanceID:    l.instanceID("HP_INSTANCE_ID", "inst-local"),
		LogLevel:      l.level("HP_LOG_LEVEL", slog.LevelInfo),
		ShutdownGrace: l.duration("HP_SHUTDOWN_GRACE", 15*time.Second),
	}
	if len(l.problems) > 0 {
		return Config{}, fmt.Errorf("invalid config: %s", strings.Join(l.problems, "; "))
	}
	return cfg, nil
}

// loader accumulates problems rather than returning on the first one.
type loader struct {
	lookup   func(string) (string, bool)
	problems []string
}

func (l *loader) reject(key, raw, why string) {
	l.problems = append(l.problems, fmt.Sprintf("%s=%q %s", key, raw, why))
}

// present reports a value only when it is set to something non-blank. Set but
// empty is a typo, not a policy, so it is rejected rather than defaulted.
func (l *loader) present(key string) (string, bool) {
	raw, ok := l.lookup(key)
	if !ok {
		return "", false
	}
	v := strings.TrimSpace(raw)
	if v == "" {
		l.reject(key, raw, "is set but empty")
		return "", false
	}
	return v, true
}

func (l *loader) str(key, def string) string {
	if v, ok := l.present(key); ok {
		return v
	}
	return def
}

func (l *loader) duration(key string, def time.Duration) time.Duration {
	v, ok := l.present(key)
	if !ok {
		return def
	}
	d, err := time.ParseDuration(v)
	switch {
	case err != nil:
		l.reject(key, v, "is not a duration")
	case d <= 0:
		l.reject(key, v, "must be positive")
	default:
		return d
	}
	return def
}

func (l *loader) level(key string, def slog.Level) slog.Level {
	v, ok := l.present(key)
	if !ok {
		return def
	}
	var lvl slog.Level
	if err := lvl.UnmarshalText([]byte(v)); err != nil {
		l.reject(key, v, "is not a log level (debug|info|warn|error)")
		return def
	}
	return lvl
}

func (l *loader) url(key, def string) string {
	v, ok := l.present(key)
	if !ok {
		return def
	}
	u, err := url.Parse(v)
	if err != nil || u.Scheme == "" || u.Host == "" {
		l.reject(key, v, "is not an absolute URL")
		return def
	}
	return strings.TrimRight(v, "/")
}

// instanceID rejects a dot because a Transfer ID is "<instance>.<random>" and
// ownership is resolved by splitting on the first one (ADR 0007).
func (l *loader) instanceID(key, def string) string {
	v, ok := l.present(key)
	if !ok {
		return def
	}
	if strings.ContainsAny(v, "./ ") {
		l.reject(key, v, "must not contain a dot, slash or space")
		return def
	}
	return v
}
