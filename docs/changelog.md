# Changelog

How Rediska was built, one milestone at a time (newest first).

## Milestone 18

Failure detection: nodes agree when one has failed (Phase 4d)

1. New `--cluster-node-timeout` flag, in milliseconds (default 15000, as in
   Redis). `scripts/cluster.sh` uses 3000, set with `NODE_TIMEOUT`
2. A node that hasn't answered for longer than the timeout is **PFAIL**:
   one node's suspicion, shown as `master,fail?` in `CLUSTER NODES`. The
   clock also starts when a node can't be reached at all, so a node that
   dies is noticed even with no PING outstanding
3. Every PING and PONG gossips about the nodes the sender suspects, and
   each of those counts as a failure report. Reports older than twice the
   timeout are dropped
4. When a node suspects another itself and, counting itself, a majority of
   masters agree, it marks the node **FAIL** (`master,fail`) and broadcasts
   a new `FAIL` message, so every node marks it at once
5. Keys in a failed node's slots get `CLUSTERDOWN Hash slot not served`
   instead of a `MOVED` to a dead node. Other slots keep working
6. `CLUSTER INFO` reports `cluster_slots_pfail`, `cluster_slots_fail`, and
   `cluster_state:fail` while a slot owner is FAIL
7. When the failed node answers again, PFAIL and FAIL are cleared
8. Fixed the `--cluster-node-timeout` help text in `rediska -h`

Try it:

    ./scripts/cluster.sh start
    lsof -ti :7003 | xargs kill            # stop node3
    sleep 5
    redis-cli -p 7001 cluster nodes        # node3: master,fail
    redis-cli -p 7001 cluster info         # cluster_state:fail
    ./scripts/cluster.sh start             # restart node3; fail clears
    ./scripts/cluster.sh stop

TODO

1. No failover: a failed node's slots stay down until it comes back
   (replicas, Phases 6 and 7)
2. Each node PINGs every other node every second. Redis PINGs about one
   random node a second, plus any it hasn't heard from in half the timeout,
   which scales better
3. Tests for failure detection

## Milestone 17

Gossip and `CLUSTER MEET`: nodes find each other (Phase 4b)

1. Bus messages carry the sender's own slots and a gossip section: a few
   random other nodes it knows (a tenth of them, at least 3), with their
   flags and ping/pong times
2. `CLUSTER MEET ip port` introduces a node. The two do a handshake (a
   `MEET`, answered by a `PONG` on a connection we opened), and from there
   gossip tells every other node about it within a second or two
3. Only a `MEET` adds a node we don't know; a node gossip mentions gets a
   handshake first. A node is trusted only once it answers on a connection
   we opened to it. A node that never answers is forgotten after 10 seconds
4. Each node announces its own slots in every message, and the others
   route by what it announces. If two nodes claim a slot, the first owner
   keeps it until slots carry config epochs
5. `CLUSTER NODES` lists nodes learned by gossip, with `handshake` until
   they answer

Try it:

    ./scripts/cluster.sh start
    redis-cli -p 7001 cluster nodes        # all three, connected

TODO

1. What nodes learn isn't saved (`nodes.conf`): if every node restarts,
   nodes added with `MEET` are gone
2. Every layout line needs at least one slot, and all slots are usually
   taken, so a new node can't join owning nothing yet

## Milestone 16

Releases with GoReleaser and Homebrew

1. Pushing a `v*` tag runs `.github/workflows/release.yml`: it tests, then
   GoReleaser builds macOS, Linux and Windows binaries (Intel and ARM) and
   publishes a GitHub Release with checksums and a changelog
2. Each release updates the cask in `vinayhpandya/homebrew-tap`, so
   `brew install vinayhpandya/tap/rediska` works
3. Added `rediska --version`, filled in at build time
4. Added an MIT `LICENSE`, shipped in every archive
5. New [Releasing](releasing.md) page; the install guide now covers
   Homebrew, downloads and `go install`

Try it:

    goreleaser release --snapshot --clean    # builds everything into dist/, publishes nothing

## Milestone 15

Cluster bus: nodes check on each other (Phase 4a)

1. Every node listens on a second port, its client port + 10000, for other
   nodes only. Messages are RESP arrays, so `nc` can talk to it
2. Each node connects to every other node in the layout, retries every
   second while one is down, and reconnects by itself when it comes back
3. Nodes send each other `PING` once a second and answer with `PONG`
4. `CLUSTER NODES` shows real ping-sent and pong-received times, and
   `connected` / `disconnected`
5. The bus's goroutines only wait and pass events on (`EvTick`, `EvLinkUp`,
   `EvMessage`, ...). The dispatch worker handles them, so cluster state
   needs no locks, like the data
6. The worker never waits on the network: messages go through a
   per-connection outbox and writer goroutine, and dials through `dialLoop`
7. `cluster/bus_test.go` runs three buses in one process under `-race`

Try it:

    ./scripts/cluster.sh start
    redis-cli -p 7001 cluster nodes        # pong times move every second
    lsof -ti :7002 | xargs kill            # node2 shows disconnected
    ./scripts/cluster.sh stop

TODO

1. No gossip or `CLUSTER MEET` yet: every node reads the full layout (Phase 4b)
2. No failure detection: a stopped node shows `disconnected`, but nodes
   don't agree that it has failed (Phase 4d)
3. Finding itself by port still only works on one machine

## Milestone 14

Fixed-layout cluster with redirects (Phase 2)

1. Each node reads the whole layout from `--cluster-config-file`
   (name, address, slot ranges) and finds itself by its port.
   A broken layout stops the node at startup
2. Before running a command, the node checks its keys:
   -> keys in different slots: `CROSSSLOT`
   -> slot owned by nobody: `CLUSTERDOWN Hash slot not served`
   -> slot owned by another node: `MOVED <slot> <host:port>`
3. AOF replay skips the cluster check, so a node always loads its own data
4. MULTI/EXEC: each command is checked when queued (MOVED -> EXECABORT),
   and all queued keys must be in one slot at EXEC (CROSSSLOT),
   so a transaction never runs halfway
5. Added `CLUSTER SLOTS`, `CLUSTER NODES`, `CLUSTER MYID`, `CLUSTER INFO`
   and the `# Cluster` section in INFO, matching real Redis 7's formats
6. `scripts/cluster.sh start|stop|status|clean` runs a local cluster
7. End-to-end test (`go test ./e2e/`) starts 3 real nodes and checks
   redirects, hash tags, pipelines, transactions and CLUSTER SLOTS
8. Verified with `redis-cli -c`, `redis-cli --cluster check` and
   `redis-benchmark --cluster`

Try it:

    ./scripts/cluster.sh start
    redis-cli -p 7001 set name vinay      # MOVED 5798 127.0.0.1:7002
    redis-cli -c -p 7001 set name vinay   # follows the redirect: OK
    redis-cli -p 7001 del age name        # CROSSSLOT
    ./scripts/cluster.sh stop

TODO

1. Nodes don't talk to each other yet; the layout is fixed until restart (Phase 4)
2. No moving slots between nodes (Phase 5), no replicas or failover (Phases 6, 7)
3. Finding itself by port only works when all nodes are on one machine;
   needs a `--cluster-node-name` flag for separate machines
4. redis-py's cluster client also needs HELLO and COMMAND, which Rediska doesn't have yet

## Milestone 13

Groundwork for cluster mode and hash slots (Phase 0 and 1)

1. Fixed AOF: INCR, APPEND, PFADD, PFMERGE and DEL are now written to the AOF
2. Added DEL command
3. Every command has a key spec (first key, last key, step) like Redis's
   command table, with a test that fails if a new command has none
4. Added `--cluster-enabled` flag
5. Hash slots: CRC16 (XMODEM) mod 16384, with `{hash tag}` support
6. Added `CLUSTER KEYSLOT`, verified against real Redis

## Milestone 12

1. Bridge gap to real redis
   -> Currenty at least 2.1x times slower than the original redis
2. Benchmarking basic commands

## Milestone 11

1. Implement LFU
2. Sampling algorithm set according to redis

## Milestone 10

1. Implement hyperloglog
2. Implement dense encoding
3. Implement PFCOUNT, PFMERGE, PFADD
4. TODO -> Implement sparse encoding for better efficiency
5. Implemented sparse and dense encoding

## Milestone 9

Implemented transactions

1. Implemented multi and exec commands for transactions
2. Implemented DISCARD as well
3. Need to verify with AOF files as well

## Milestone 8

Implemented graceful shutdown redis

1. Implemented graceful shutdown for redis with filesync for aof
2. All inflight requests will be completed within a deadline
3. All requests will be tracked using waitgroups to monitor in fligt
   requests
4. TODO RDB sync snapshots

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

## Milestone 6

Add Active expiry and lazy expiry for expired objects

1. Allow server to delete expired keys
2. Allow lazy expiry on keys which are deleted
3. TODO Add Compaction to aof for expire as well

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

## Milestone 4

Add a single go-routine to handle and execute the commands

1. Remove sync locks on hashmap to store data
2. Have a single go routing and channel (buffered)
   channels to check for data and execute them

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
