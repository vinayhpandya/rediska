# learning-redis
This repository is for writing down all the essential details about redis and how it will be implemented in go or any other language

## Milestone 1
Build a simple tcp echo server which serves echoes the command back to the user
Running the server
```bash
  go run . --host 127.0.0.1 --port 7973
```

Verifying the echo command
```bash
nc 127.0.0.1 7379
HELLO
```

## Milestone 2 
Build a simple RESP parser which can understand the commands coming from redis cli

From the client run the redis cli and connect it to the port where the server is
running
```bash
redis-cli -p 7379 set k v
```
On the server we see that the commands are read and parsed
```bash
go run . --host 127.0.0.1 --port 7379
Starting Rediska on host 127.0.0.1 and port 7379 
rediska is listening on 127.0.0.1:7379 
2026/05/14 11:37:59 client connected: 127.0.0.1:51367
2026/05/14 11:37:59 received from 127.0.0.1:51367: []interface {}{"ping"}
2026/05/14 11:37:59 client disconnected: 127.0.0.1:51367 
2026/05/14 11:38:26 client connected: 127.0.0.1:51416
2026/05/14 11:38:26 received from 127.0.0.1:51416: []interface {}{"set", "k", "v"}
2026/05/14 11:38:26 client disconnected: 127.0.0.1:51416 
```

## Milestone 3
Implementing the PING command and setting up command registry

```bash
go run . --host 127.0.0.1 --port 7379
```

For the client
```bash
redis-cli -p 7379 PING "Vinay"         
"Vinay"
```

Using telnet we get
```bash
printf '*1\r\n$4\r\nPING\r\n' | nc localhost 7379 
+PONG
```

## Milestone 4
Add a single go-routine to handle and execute the commands

1. Remove sync locks on hashmap to store data
2. Have a single go routing and channel (buffered)
   channels to check for data and execute them

## Milestone 5
Add AOF functionality in rediska

1. Allow redis to recover from AOF file once server crashes or shuts down
2. Currently it is set on every write command instead of
   BGWRITETOAOF
3. TODO: Need to implement DEL and cleanup to avoid AOF getting too big and
   also run compaction like every second

```bash
 go run . --host 127.0.0.1 --port 7379 --appendOnly=true --appendOnlyFile=/tmp/tempfile.aof
```

## Milestone 6
Add Active expiry and lazy expiry for expired objects

1. Allow server to delete expired keys
2. Allow lazy expiry on keys which are deleted
3. TODO Add Compaction to aof for expire as well

## Milestone 7
Change redis encoding and add INCR

1. Add Object encoding
2. Add INCR functionality

### Milestone 7.1

Add INFO command and monitoring

1. Add tracing and prometheus
2. Add redis exporter

Redis exporter will call INFO on redis cli and get keyspace details
and export it to prometheus
Grafana will render the prometheus data

### Milestone 7.2

Add DEBUG command (for memory) and implement approximate LRU

1. Verified with load testing by monitoring garafana that the memory
   is always under `maxmemory`
2. Verified that there is no OOM error once we hit 1MB under lru
3. Tested no-eviction policy as well

## Milestone 8

Implemented graceful shutdown redis

1. Implemented graceful shutdown for redis with filesync for aof
2. All inflight requests will be completed within a deadline
3. All requests will be tracked using waitgroups to monitor in fligt
   requests
4. TODO RDB sync snapshots

## Milestone 9
Implemented transactions

1. Implemented multi and exec commands for transactions
2. Implemented DISCARD as well
3. Need to verify with AOF files as well

## Milestone 10

1. Implement hyperloglog
2. Implement dense encoding
3. Implement PFCOUNT, PFMERGE, PFADD
4. TODO -> Implement sparse encoding for better efficiency
5. Implemented sparse and dense encoding

## Milestone 11

1. Implement LFU
2. Sampling algorithm set according to redis

## Milestone 12

1. Bridge gap to real redis
   -> Currenty at least 2.1x times slower than the original redis
2. Benchmarking basic commands

## Performance work: profiling and closing the gap with Redis

Benchmarked with `redis-benchmark` against real Redis on the same machine.
Started at roughly half of Redis's throughput and 2x its latency; profiled
with `pprof` at each step rather than guessing, and closed the gap to near
parity under pipelined load.

### 1. Removed unconditional per-command debug logging
Two `log.Printf` calls ran on the hot path for every single command
(`tcp_server.go`), accounting for ~19% of CPU time under load. Removed.

### 2. Replaced per-command read-deadline polling with a ctx-watcher goroutine
The connection loop was calling `conn.SetReadDeadline` twice per command
(~5% of CPU) just to support graceful shutdown. Replaced with a single
per-connection goroutine that closes the connection directly when the
server's context is cancelled -- removes the syscalls entirely and makes
shutdown faster, not slower.

### 3. Pooled reply channels instead of allocating one per command
Each command allocated a fresh `chan []byte` to receive its reply from the
dispatch worker. Replaced with a `sync.Pool` (~4.7% of CPU).

### 4. Reduced RESP bulk-string parsing overhead
Combined the body read and trailing `\r\n` check into a single `io.ReadFull`
call (previously: one `ReadFull` + two separate `ReadByte` calls), and added
a per-connection reusable scratch buffer so decoding many commands on one
connection doesn't allocate a fresh `[]byte` for every argument.

**Result of #1-4:** roughly 2x throughput, gap to Redis closed from ~2x to
~1.18x (single-command, non-pipelined benchmarking).

### 5. Batched pipelined commands into a single dispatch round-trip
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

### Tooling
Profiled with Go's built-in `net/http/pprof` (enabled via a `--pprof-addr`
flag, off by default) and `go tool pprof -http=...` for interactive CPU/
allocation flame graphs and call graphs.