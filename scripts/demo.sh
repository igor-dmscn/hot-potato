#!/usr/bin/env bash
# A whole Transfer, end to end, with two spuds and no browser.
#
# Starts a server, signs up two Users, sends a folder from one to the other, and
# checks the archive that comes out the far end. Everything lands in a temporary
# directory and is cleaned up.
set -euo pipefail

ADDR=${ADDR:-127.0.0.1:8099}
BASE="http://$ADDR"
# The server has no default for this on purpose, so supply the local one here
# too: this script is also run directly, not only through make.
export HP_DATABASE_URL=${HP_DATABASE_URL:-postgres://hotpotato:hotpotato@localhost:5432/hotpotato}
WORK=$(mktemp -d)
SUFFIX=$RANDOM

cleanup() {
	[[ -n ${SERVER_PID:-} ]] && kill "$SERVER_PID" 2>/dev/null || true
	[[ -n ${RECV_PID:-} ]] && kill "$RECV_PID" 2>/dev/null || true
	rm -rf "$WORK"
}
trap cleanup EXIT

echo "==> starting a server on $ADDR"
HP_ADDR="$ADDR" HP_LOG_LEVEL=warn ./bin/server >"$WORK/server.log" 2>&1 &
SERVER_PID=$!
for _ in $(seq 30); do
	curl -fsS "$BASE/healthz" >/dev/null 2>&1 && break
	sleep 0.5
done
curl -fsS "$BASE/healthz" >/dev/null || { echo "the server never came up:"; cat "$WORK/server.log"; exit 1; }

echo "==> building a folder to send"
mkdir -p "$WORK/docs/nested"
head -c 400000 /dev/urandom >"$WORK/docs/a.bin"
head -c 300000 /dev/urandom >"$WORK/docs/nested/b.bin"
printf 'the server cannot hold what it is handed\n' >"$WORK/docs/nested/note.txt"
mkdir -p "$WORK/inbox"

echo "==> bea waits for an offer"
./bin/spud -base "$BASE" -email "bea$SUFFIX@example.com" -password hunter2hunter2 \
	-name "bea$SUFFIX" -signup recv "$WORK/inbox" >"$WORK/recv.log" 2>&1 &
RECV_PID=$!
sleep 1

echo "==> ana sends"
./bin/spud -base "$BASE" -email "ana$SUFFIX@example.com" -password hunter2hunter2 \
	-name "ana$SUFFIX" -signup send "bea$SUFFIX" "$WORK/docs"

wait "$RECV_PID" || true
RECV_PID=

echo
echo "==> what bea saw"
sed 's/^/    /' "$WORK/recv.log"

echo
echo "==> the archive"
python3 - "$WORK/inbox/docs.zip" <<'PY'
import sys, zipfile
z = zipfile.ZipFile(sys.argv[1])
for i in z.infolist():
	print(f"    {i.filename:32} {i.file_size:>9,} bytes  method={'store' if i.compress_type == 0 else i.compress_type}")
print("    crc:", z.testzip() or "every entry checks out")
PY

echo
echo "==> what the server counted"
curl -fsS "$BASE/metrics" | grep -E '^(hp_relay_bytes_total|hp_transfers_total|hp_streams_active) ' | sed 's/^/    /'
echo
echo "done."
