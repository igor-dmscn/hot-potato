#!/usr/bin/env bash
# Ten thousand idle Streams, and the graceful drain with all of them open.
#
# This is the phase 10 measurement in docs/load-test.md. Raise `ulimit -n` before
# blaming Go: the failure mode of a low limit is "accept: too many open files",
# which looks like the server refusing connections rather than the shell refusing
# to lend it file descriptors.
set -euo pipefail

ADDR=${ADDR:-127.0.0.1:8099}
BASE="http://$ADDR"
STREAMS=${STREAMS:-10000}
# The server has no default for this on purpose, so supply the local one here
# too: this script is also run directly, not only through make.
export HP_DATABASE_URL=${HP_DATABASE_URL:-postgres://hotpotato:hotpotato@localhost:5432/hotpotato}
WORK=$(mktemp -d)
SUFFIX=$RANDOM

cleanup() {
	[[ -n ${LOAD_PID:-} ]] && kill "$LOAD_PID" 2>/dev/null || true
	[[ -n ${SERVER_PID:-} ]] && kill "$SERVER_PID" 2>/dev/null || true
	rm -rf "$WORK"
}
trap cleanup EXIT

if [[ $(ulimit -n) -lt $((STREAMS + 1000)) ]]; then
	echo "ulimit -n is $(ulimit -n); $STREAMS streams need more. Try: ulimit -n 65535" >&2
	exit 1
fi

sample() {
	curl -fsS "$BASE/metrics" | grep -E \
		'^(go_goroutines|go_threads|process_open_fds|process_resident_memory_bytes|hp_streams_active|go_memstats_heap_alloc_bytes) ' |
		sed "s/^/    $1  /"
}

echo "==> starting a server on $ADDR"
HP_ADDR="$ADDR" HP_LOG_LEVEL=info ./bin/server >"$WORK/server.log" 2>&1 &
SERVER_PID=$!
for _ in $(seq 30); do
	curl -fsS "$BASE/healthz" >/dev/null 2>&1 && break
	sleep 0.5
done
curl -fsS "$BASE/healthz" >/dev/null || { echo "the server never came up:"; cat "$WORK/server.log"; exit 1; }

echo "==> baseline"
sample baseline

echo "==> opening $STREAMS Streams"
./bin/spud -base "$BASE" -email "load$SUFFIX@example.com" -password hunter2hunter2 \
	-name "loadbot$SUFFIX" -signup -streams "$STREAMS" load >"$WORK/load.log" 2>&1 &
LOAD_PID=$!

for _ in $(seq 120); do
	open=$(curl -fsS --max-time 5 "$BASE/metrics" | awk '/^hp_streams_active /{print int($2)}')
	[[ ${open:-0} -ge $STREAMS ]] && break
	sleep 2
done
open=$(curl -fsS "$BASE/metrics" | awk '/^hp_streams_active /{print int($2)}')
if [[ ${open:-0} -lt $STREAMS ]]; then
	echo "only $open of $STREAMS Streams opened:" >&2
	tail -5 "$WORK/load.log" >&2
	exit 1
fi

echo "==> with $open Streams open"
sample "  loaded"
echo "    GC:"
curl -fsS "$BASE/metrics" | grep -E '^go_gc_duration_seconds(\{|_count)' | sed 's/^/      /'

echo
echo "==> SIGTERM with all $open of them open"
started=$(date +%s%N)
kill -TERM "$SERVER_PID"
while kill -0 "$SERVER_PID" 2>/dev/null; do sleep 0.02; done
ended=$(date +%s%N)
SERVER_PID=

echo "    shutdown: $(((ended - started) / 1000000)) ms"
echo "    server said:"
grep -E 'draining' "$WORK/server.log" | sed 's/^/      /'
echo "    client said:"
tail -2 "$WORK/load.log" | sed 's/^/      /'
echo
echo "Without the drain hook that number is not slow, it is never (Go issue #41344)."
