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
	"slices"
	"strconv"
	"strings"
	"time"
)

// Config is the resolved configuration. Load either returns a fully populated
// Config or an error naming everything wrong with the environment — one
// restart, not three.
type Config struct {
	Addr string // HP_ADDR, default ":8080"
	// HP_TLS_CERT and HP_TLS_KEY. Empty serves plain HTTP/1.1. Set, and Go
	// negotiates HTTP/2 over ALPN, which is the only way a browser gets more than
	// six connections to this origin — no browser speaks cleartext h2c.
	TLSCert       string
	TLSKey        string
	ExternalURL   string        // HP_EXTERNAL_URL — how other instances reach this one
	InstanceID    string        // HP_INSTANCE_ID — the prefix of every Transfer ID it owns
	LogLevel      slog.Level    // HP_LOG_LEVEL
	ShutdownGrace time.Duration // HP_SHUTDOWN_GRACE

	DatabaseURL        Secret        // HP_DATABASE_URL
	SessionTTL         time.Duration // HP_SESSION_TTL
	LoginMaxFailures   int           // HP_LOGIN_MAX_FAILURES, per IP+email
	LoginFailureWindow time.Duration // HP_LOGIN_FAILURE_WINDOW

	// Every timing constant in the policy envelope is a field here rather than a
	// literal somewhere. That is what lets the presence and reaper tests run in
	// milliseconds instead of sleeping for ten seconds (docs/protocol.md).
	SSEHeartbeat     time.Duration // HP_SSE_HEARTBEAT
	SSERetry         time.Duration // HP_SSE_RETRY — the client's reconnect hint
	SSEWriteDeadline time.Duration // HP_SSE_WRITE_DEADLINE — per write, not per stream
	StreamBuffer     int           // HP_STREAM_BUFFER — events before a Stream is dropped
	BusBuffer        int           // HP_BUS_BUFFER
	PresenceGrace    time.Duration // HP_PRESENCE_GRACE

	OfferTTL          time.Duration // HP_OFFER_TTL
	MaxOutbound       int           // HP_MAX_OUTBOUND — non-terminal Transfers per Sender
	MaxPendingInbound int           // HP_MAX_PENDING_INBOUND — unanswered offers per Recipient
	MaxPayloadBytes   int64         // HP_MAX_PAYLOAD_BYTES
	MaxEntries        int           // HP_MAX_ENTRIES — files in one folder
	OfferRate         int           // HP_OFFER_RATE per HP_OFFER_RATE_WINDOW
	OfferRateWindow   time.Duration // HP_OFFER_RATE_WINDOW
	ReapInterval      time.Duration // HP_REAP_INTERVAL
	TerminalWindow    time.Duration // HP_TERMINAL_WINDOW — how long finished Transfers stay in snapshots

	RendezvousWait     time.Duration // HP_RENDEZVOUS_WAIT — how long a parked Recipient waits
	RelayWriteDeadline time.Duration // HP_RELAY_WRITE_DEADLINE — per write, not per relay
	RelayBuffer        int           // HP_RELAY_BUFFER
	ProgressInterval   time.Duration // HP_PROGRESS_INTERVAL
	ResumeWindow       time.Duration // HP_RESUME_WINDOW — 0 makes an interruption terminal

	// Distributed. With HP_REDIS_URL unset the process runs standalone: presence
	// and the Transfer read model stay in memory and nothing is shared.
	Bus          string // HP_BUS: memory | nats | redis | kafka
	NATSURL      string // HP_NATS_URL
	KafkaBrokers string // HP_KAFKA_BROKERS, comma-separated
	BusSubject   string // HP_BUS_SUBJECT — subject, channel or topic
	RedisURL     string // HP_REDIS_URL

	OTLPEndpoint string        // HP_OTLP_ENDPOINT — empty leaves tracing a no-op
	TraceSample  float64       // HP_TRACE_SAMPLE, 0–1
	ReadyTimeout time.Duration // HP_READY_TIMEOUT — the whole /readyz budget

	PresenceTTL     time.Duration // HP_PRESENCE_TTL
	PresenceRefresh time.Duration // HP_PRESENCE_REFRESH
	InstanceTTL     time.Duration // HP_INSTANCE_TTL
	InstanceRefresh time.Duration // HP_INSTANCE_REFRESH
	ReadModelTTL    time.Duration // HP_READ_MODEL_TTL
}

// Distributed reports whether this process shares state with others.
func (c Config) Distributed() bool { return c.RedisURL != "" }

// Secret is a configuration value that must never reach a log or a response.
//
// String and LogValue cover fmt and slog's text handler. MarshalJSON is the
// one that actually matters here: slog's JSON handler marshals a struct field
// with encoding/json, which consults neither of the other two.
type Secret string

const redacted = "[redacted]"

func (Secret) String() string               { return redacted }
func (Secret) LogValue() slog.Value         { return slog.StringValue(redacted) }
func (Secret) MarshalJSON() ([]byte, error) { return []byte(`"` + redacted + `"`), nil }
func (s Secret) Reveal() string             { return string(s) }

// Load reads configuration through lookup, which has os.LookupEnv's signature.
// Injecting it instead of reading the process environment keeps these tests
// pure and parallel: t.Setenv forbids t.Parallel, and a global environment is
// shared mutable state between tests.
func Load(lookup func(string) (string, bool)) (Config, error) {
	l := loader{lookup: lookup}
	cfg := Config{
		Addr:          l.str("HP_ADDR", ":8080"),
		TLSCert:       l.str("HP_TLS_CERT", ""),
		TLSKey:        l.str("HP_TLS_KEY", ""),
		ExternalURL:   l.url("HP_EXTERNAL_URL", "http://localhost:8080"),
		InstanceID:    l.instanceID("HP_INSTANCE_ID", "inst-local"),
		LogLevel:      l.level("HP_LOG_LEVEL", slog.LevelInfo),
		ShutdownGrace: l.duration("HP_SHUTDOWN_GRACE", 15*time.Second),

		// No default: see the require calls below.
		DatabaseURL:        Secret(l.str("HP_DATABASE_URL", "")),
		SessionTTL:         l.duration("HP_SESSION_TTL", 7*24*time.Hour),
		LoginMaxFailures:   l.count("HP_LOGIN_MAX_FAILURES", 10),
		LoginFailureWindow: l.duration("HP_LOGIN_FAILURE_WINDOW", 15*time.Minute),

		SSEHeartbeat:     l.duration("HP_SSE_HEARTBEAT", 15*time.Second),
		SSERetry:         l.duration("HP_SSE_RETRY", 3*time.Second),
		SSEWriteDeadline: l.duration("HP_SSE_WRITE_DEADLINE", 10*time.Second),
		StreamBuffer:     l.count("HP_STREAM_BUFFER", 32),
		BusBuffer:        l.count("HP_BUS_BUFFER", 256),
		PresenceGrace:    l.duration("HP_PRESENCE_GRACE", 10*time.Second),

		OfferTTL:          l.duration("HP_OFFER_TTL", time.Minute),
		MaxOutbound:       l.count("HP_MAX_OUTBOUND", 3),
		MaxPendingInbound: l.count("HP_MAX_PENDING_INBOUND", 10),
		MaxPayloadBytes:   l.bytes("HP_MAX_PAYLOAD_BYTES", 10<<30),
		MaxEntries:        l.count("HP_MAX_ENTRIES", 10_000),
		OfferRate:         l.count("HP_OFFER_RATE", 10),
		OfferRateWindow:   l.duration("HP_OFFER_RATE_WINDOW", time.Minute),
		ReapInterval:      l.duration("HP_REAP_INTERVAL", 5*time.Second),
		TerminalWindow:    l.duration("HP_TERMINAL_WINDOW", time.Minute),

		RendezvousWait:     l.duration("HP_RENDEZVOUS_WAIT", 30*time.Second),
		RelayWriteDeadline: l.duration("HP_RELAY_WRITE_DEADLINE", 30*time.Second),
		RelayBuffer:        l.count("HP_RELAY_BUFFER", 64<<10),
		ProgressInterval:   l.duration("HP_PROGRESS_INTERVAL", 250*time.Millisecond),
		ResumeWindow:       l.duration("HP_RESUME_WINDOW", 30*time.Second),

		Bus:          l.oneOf("HP_BUS", "memory", "memory", "nats", "redis", "kafka"),
		NATSURL:      l.str("HP_NATS_URL", "nats://localhost:4222"),
		KafkaBrokers: l.str("HP_KAFKA_BROKERS", "localhost:9092"),
		BusSubject:   l.str("HP_BUS_SUBJECT", "hp.events"),
		RedisURL:     l.str("HP_REDIS_URL", ""),

		OTLPEndpoint: l.str("HP_OTLP_ENDPOINT", ""),
		TraceSample:  l.ratio("HP_TRACE_SAMPLE", 1),
		ReadyTimeout: l.duration("HP_READY_TIMEOUT", 2*time.Second),

		PresenceTTL:     l.duration("HP_PRESENCE_TTL", 30*time.Second),
		PresenceRefresh: l.duration("HP_PRESENCE_REFRESH", 10*time.Second),
		InstanceTTL:     l.duration("HP_INSTANCE_TTL", 30*time.Second),
		InstanceRefresh: l.duration("HP_INSTANCE_REFRESH", 10*time.Second),
		ReadModelTTL:    l.duration("HP_READ_MODEL_TTL", 5*time.Minute),
	}

	// Values with no safe default, checked after the literal so a conditional
	// requirement can read what it depends on. There is deliberately no
	// HP_ENV=prod switch to turn these on: a profile fails open, and forgetting
	// to set it in production reinstates every development default silently.
	// Requirements derived from values already present cannot be forgotten.

	// A default here compiles the development password into the binary, and a
	// typo in the variable's name in production becomes a server quietly running
	// against localhost instead of one that refuses to start.
	l.require("HP_DATABASE_URL")
	// Half a key pair is a deployment that thinks it has TLS.
	if cfg.TLSCert != "" {
		l.require("HP_TLS_KEY")
	}
	if cfg.TLSKey != "" {
		l.require("HP_TLS_CERT")
	}
	if cfg.Distributed() {
		// Two instances both answering to "inst-local" break ownership routing: a
		// Transfer ID is "<instance>.<random>" and every other instance answers
		// 307 to the owner it names (ADR 0007).
		l.require("HP_INSTANCE_ID")
		// The client is what follows the 307, so a container-internal default
		// redirects a browser to a host it cannot resolve.
		l.require("HP_EXTERNAL_URL")
	}
	// A localhost default for a broker is the same trap as the database one.
	switch cfg.Bus {
	case "nats":
		l.require("HP_NATS_URL")
	case "kafka":
		l.require("HP_KAFKA_BROKERS")
	case "redis":
		// openBus rejects this too, but one message listing everything wrong
		// beats a second restart to find the next thing.
		l.require("HP_REDIS_URL")
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

// require reports a key that has no safe default. present has already rejected
// a set-but-empty value, so absence is all that is left to catch.
func (l *loader) require(key string) {
	if _, ok := l.lookup(key); !ok {
		l.problems = append(l.problems, key+" is required")
	}
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

// count parses a positive integer. A zero limit is a typo, not a policy.
func (l *loader) count(key string, def int) int {
	v, ok := l.present(key)
	if !ok {
		return def
	}
	n, err := strconv.Atoi(v)
	switch {
	case err != nil:
		l.reject(key, v, "is not a number")
	case n <= 0:
		l.reject(key, v, "must be positive")
	default:
		return n
	}
	return def
}

// bytes parses a plain byte count. No "10GB" suffixes: one syntax to get wrong
// is enough, and this value is set once per deployment.
func (l *loader) bytes(key string, def int64) int64 {
	v, ok := l.present(key)
	if !ok {
		return def
	}
	n, err := strconv.ParseInt(v, 10, 64)
	switch {
	case err != nil:
		l.reject(key, v, "is not a byte count")
	case n <= 0:
		l.reject(key, v, "must be positive")
	default:
		return n
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

// ratio parses a fraction between 0 and 1.
func (l *loader) ratio(key string, def float64) float64 {
	v, ok := l.present(key)
	if !ok {
		return def
	}
	f, err := strconv.ParseFloat(v, 64)
	switch {
	case err != nil:
		l.reject(key, v, "is not a number")
	case f < 0 || f > 1:
		l.reject(key, v, "must be between 0 and 1")
	default:
		return f
	}
	return def
}

// oneOf accepts a value from a fixed set, and names the set when it does not.
func (l *loader) oneOf(key, def string, allowed ...string) string {
	v, ok := l.present(key)
	if !ok {
		return def
	}
	if slices.Contains(allowed, v) {
		return v
	}
	l.reject(key, v, "must be one of "+strings.Join(allowed, ", "))
	return def
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
