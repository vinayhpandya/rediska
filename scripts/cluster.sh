#!/usr/bin/env bash
# Run a local Rediska cluster: N nodes on one machine, each owning an equal
# share of the 16384 slots.
#
#   ./scripts/cluster.sh start    build rediska, write cluster.conf, start the nodes
#   ./scripts/cluster.sh stop     stop the nodes (data stays in their AOF files)
#   ./scripts/cluster.sh status   show which nodes are running
#   ./scripts/cluster.sh clean    stop, and delete all their files
#
# Settings (environment variables):
#   NODES=3          how many nodes
#   BASE_PORT=7001   first port; nodes use BASE_PORT, BASE_PORT+1, ...
#   DIR=/tmp/rediska-cluster   where cluster.conf, logs, AOFs and pids go
#
# Written for the bash 3.2 that ships with macOS, so no newer bash features.
set -euo pipefail

NODES=${NODES:-3}
BASE_PORT=${BASE_PORT:-7001}
DIR=${DIR:-/tmp/rediska-cluster}

# Always work from the repo root, wherever the script is called from.
cd "$(dirname "$0")/.."
BIN="$PWD/rediska"

ports() {
	local i
	for ((i = 0; i < NODES; i++)); do echo $((BASE_PORT + i)); done
}

is_running() { # port
	local pidfile="$DIR/node-$1.pid"
	[ -f "$pidfile" ] && kill -0 "$(cat "$pidfile")" 2>/dev/null
}

# write_conf splits the slots the same way `redis-cli --cluster create`
# does, so 3 nodes get 0-5460, 5461-10922, 10923-16383.
write_conf() {
	awk -v n="$NODES" -v base="$BASE_PORT" 'BEGIN {
		per = 16384 / n; cursor = 0; first = 0
		print "# Written by scripts/cluster.sh: name, address, slots"
		for (i = 0; i < n; i++) {
			last = int(cursor + per - 1 + 0.5)          # round()
			if (last > 16383 || i == n - 1) last = 16383
			printf "node%d  127.0.0.1:%d  %d-%d\n", i + 1, base + i, first, last
			first = last + 1; cursor += per
		}
	}' >"$DIR/cluster.conf"
}

start() {
	mkdir -p "$DIR"
	echo "building rediska..."
	go build -o "$BIN" .

	# Only write cluster.conf the first time, so a restart keeps the same
	# layout (each node's AOF only holds keys for the slots it owned).
	[ -f "$DIR/cluster.conf" ] || write_conf

	for p in $(ports); do
		if is_running "$p"; then
			echo "node on $p is already running"
			continue
		fi
		"$BIN" --port "$p" --cluster-enabled --cluster-config-file "$DIR/cluster.conf" \
			--appendOnly --appendOnlyFile "$DIR/node-$p.aof" \
			>"$DIR/node-$p.log" 2>&1 &
		echo $! >"$DIR/node-$p.pid"
	done

	# Wait until every node answers PING (or give up after ~5 seconds).
	for p in $(ports); do
		for _ in 1 2 3 4 5 6 7 8 9 10; do
			redis-cli -p "$p" ping >/dev/null 2>&1 && break
			sleep 0.5
		done
		if ! redis-cli -p "$p" ping >/dev/null 2>&1; then
			echo "node on $p did not start; last lines of its log:"
			tail -5 "$DIR/node-$p.log"
			exit 1
		fi
	done

	echo
	cat "$DIR/cluster.conf"
	echo
	echo "cluster is up. Try:  redis-cli -c -p $BASE_PORT"
	echo "files are in $DIR"
}

stop() {
	for p in $(ports); do
		if is_running "$p"; then
			kill "$(cat "$DIR/node-$p.pid")"
			# Wait for it to exit: on shutdown rediska flushes its AOF to
			# disk, and a quick restart must not race that.
			for _ in 1 2 3 4 5 6 7 8 9 10; do
				is_running "$p" || break
				sleep 0.5
			done
			echo "stopped node on $p"
		fi
		rm -f "$DIR/node-$p.pid"
	done
}

status() {
	for p in $(ports); do
		if is_running "$p"; then
			echo "$p  running  $(redis-cli -p "$p" info keyspace 2>/dev/null | grep db0 || true)"
		else
			echo "$p  stopped"
		fi
	done
}

clean() {
	stop
	rm -rf "$DIR"
	echo "removed $DIR"
}

case "${1:-}" in
start) start ;;
stop) stop ;;
status) status ;;
clean) clean ;;
*)
	echo "usage: $0 start|stop|status|clean" >&2
	exit 1
	;;
esac