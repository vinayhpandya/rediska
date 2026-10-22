# Performance

Profiling and closing the gap with Redis.

Benchmarked with `redis-benchmark` against real Redis on the same machine.
Started at roughly half of Redis's throughput and 2x its latency; profiled
with `pprof` at each step rather than guessing, and closed the gap to near
parity under pipelined load.

## 1. Removed unconditional per-command debug logging

Two `log.Printf` calls ran on the hot path for every single command
(`tcp_server.go`), accounting for ~19% of CPU time under load. Removed.

## 2. Replaced per-command read-deadline polling with a ctx-watcher goroutine

The connection loop was calling `conn.SetReadDeadline` twice per command
(~5% of CPU) just to support graceful shutdown. Replaced with a single
per-connection goroutine that closes the connection directly when the
server's context is cancelled -- removes the syscalls entirely and makes
shutdown faster, not slower.

## 3. Pooled reply channels instead of allocating one per command

Each command allocated a fresh `chan []byte` to receive its reply from the
dispatch worker. Replaced with a `sync.Pool` (~4.7% of CPU).

## 4. Reduced RESP bulk-string parsing overhead

Combined the body read and trailing `\r\n` check into a single `io.ReadFull`
call (previously: one `ReadFull` + two separate `ReadByte` calls), and added
a per-connection reusable scratch buffer so decoding many commands on one
connection doesn't allocate a fresh `[]byte` for every argument.

**Result of #1-4:** roughly 2x throughput, gap to Redis closed from ~2x to
~1.18x (single-command, non-pipelined benchmarking).

## 5. Batched pipelined commands into a single dispatch round-trip

Profiling with `redis-benchmark -P 16` (pipelining) revealed a different
bottleneck: every command -- pipelined or not -- was sent through the
single dispatch worker as its own message on a Go channel, each paying a
full goroutine park/wake + lock cycle. At pipelined request rates this
channel contention became the dominant cost, confirmed via CPU profile
(`pthread_cond_wait`/`signal` and channel-lock functions dominating the
profile).

Fixed by accumulating consecutive plain commands already sitting in the
connection's read buffer (`bufio.Reader.Buffered() > 0`, i.e. commands the
client already sent without waiting for a reply) into one batch, dispatched
through the channel as a single request. `MULTI`/`EXEC` transactions and
malformed frames are handled correctly as batch boundaries -- verified with
targeted tests, not just benchmarked.

**Result:** pipelined throughput went from ~60k rps to 300k+ rps in local
testing, closing the previously channel-contention-dominated gap.

## Tooling

Profiled with Go's built-in `net/http/pprof` (enabled via a `--pprof-addr`
flag, off by default) and `go tool pprof -http=...` for interactive CPU/
allocation flame graphs and call graphs.
