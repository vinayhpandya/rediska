#!/usr/bin/env bash
# Checks that the AOF survives a restart correctly (PR 0).
# Run it from the root of the rediska repo:  bash scripts/aof_check.sh
set -euo pipefail

PORT=7399
AOF=/tmp/rediska_aof_check.aof
LOG=/tmp/rediska_aof_check.log
cli() { redis-cli -p "$PORT" "$@"; }

# Print the AOF one command per line instead of raw RESP.
show_aof() {
  tr -d '\r' < "$AOF" | awk '
    /^\*/ { if (line != "") print "  " line; line = ""; next }
    /^\$/ { next }
          { line = (line == "" ? $0 : line " " $0) }
    END   { if (line != "") print "  " line }'
}

start() {
  ./rediska-aof-check --host 127.0.0.1 --port "$PORT" \
    --appendOnly=true --appendOnlyFile="$AOF" >>"$LOG" 2>&1 &
  PID=$!
  for _ in $(seq 50); do cli PING >/dev/null 2>&1 && return; sleep 0.1; done
  echo "server didn't start; see $LOG"; exit 1
}

stop() {
  kill -INT "$PID"          # same as Ctrl+C: flushes the AOF, then exits
  wait "$PID" 2>/dev/null || true
}

check() { # check <description> <actual> <expected>
  if [ "$2" == "$3" ]; then echo "  PASS  $1"
  else echo "  FAIL  $1: got '$2', want '$3'"; FAILED=1; fi
}

FAILED=0
rm -f "$AOF" "$LOG"
go build -o rediska-aof-check .
trap 'kill "$PID" 2>/dev/null || true; rm -f rediska-aof-check' EXIT

echo "== 1. Writing keys"
start
cli SET session abc EX 2 >/dev/null   # expires in 2 s; we'll read it after
cli SET k v >/dev/null
cli EXPIRE k 2 >/dev/null              # expires in 2 s; we won't read it
cli SET forever yes >/dev/null         # no expiry
cli SET gone x >/dev/null
cli DEL gone >/dev/null                # an explicit DEL
sleep 3
check "GET session after it expired" "$(cli GET session)" ""

echo
echo "== 2. What the AOF recorded"
show_aof

echo
echo "== 3. Restarting from the AOF"
stop
start
check "session stays deleted"     "$(cli GET session)" ""
check "k stays deleted"           "$(cli GET k)"       ""
check "gone stays deleted"        "$(cli GET gone)"    ""
check "forever is still there"    "$(cli GET forever)" "yes"
check "forever still has no TTL"  "$(cli TTL forever)" "-1"

echo
echo "== 4. A deadline isn't pushed back by a restart"
cli SET later v EX 4 >/dev/null
sleep 2
stop
start
ttl=$(cli TTL later)
if [ "$ttl" -le 2 ] && [ "$ttl" -ge 1 ]; then echo "  PASS  TTL after restart is $ttl s (≤ 2 s left)"
else echo "  FAIL  TTL after restart is $ttl s; expected about 2 (a fresh 4 means the bug is back)"; FAILED=1; fi
stop

echo
if [ "$FAILED" == 0 ]; then echo "All checks passed."; else echo "Some checks FAILED (server log: $LOG)"; exit 1; fi