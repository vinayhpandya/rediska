# Design decisions

Why Rediska is built the way it is. Each section says what was chosen,
why, and what it costs.

## How a command flows

```mermaid
flowchart LR
    C1[client] --> G1[connection goroutine]
    C2[client] --> G2[connection goroutine]
    G1 -- "command / pipeline / MULTI batch" --> CH[(channel)]
    G2 --> CH
    E[active expiry, every 100 ms] --> CH
    CH --> W[dispatch worker]
    W --> D[Dispatch:<br/>cluster check, memory check,<br/>handler, AOF]
    D --> S[(store)]
```

1. Each client connection has its own goroutine. It reads and parses RESP,
   handles `MULTI` queuing, and groups pipelined commands.
2. Every command that touches data is sent over one channel to a single
   **dispatch worker**.
3. The worker runs commands one at a time: cluster check, memory check,
   the command's handler, then the AOF write. The reply goes back to the
   connection.

## One worker goroutine for all data

**Choice:** every command runs on one goroutine, like Redis's single
thread, instead of locking the store.

**Why:** no locks in the store at all, and commands are atomic for free:
nothing can interleave with a command, or with an `EXEC` batch. It also
mirrors how Redis really works, which is the point of the project.

**Cost:** one CPU core does all the data work. The channel was the main
bottleneck under pipelined load, which is why pipelined commands are now
sent to the worker as one batch (see [Performance](performance.md)).

## Compact value encodings

**Choice:** like Redis, a value is stored as a number (`int`) if it parses
as one, and as a string (`embstr` up to 44 bytes, `raw` above) otherwise.
Each stored value is a small tagged struct, not a Go `any`.

**Why:** saves memory, and avoids an allocation per `SET` that boxing into
an interface would cost. `OBJECT ENCODING` shows which one a key uses.

## Approximate LRU and LFU by sampling

**Choice:** eviction samples a few keys (`--maxmemory-samples`, default 5)
and evicts the best candidate from a small pool, like Redis.

**Why:** exact LRU needs a linked list across every key, which costs memory
and time on every access. Sampling gets close to exact behaviour at a tiny
cost, which is why Redis does it.

## Cluster mode

### Copy Redis Cluster's design

**Choice:** 16384 slots, CRC16 hashing, `{hash tags}`, `MOVED` and
`CROSSSLOT`, and the `CLUSTER` commands, all matching Redis 7's formats
word for word.

**Why:** existing tools (`redis-cli -c`, `redis-benchmark --cluster`,
`redis-cli --cluster check`) work against Rediska unchanged, and each piece
maps to a part of Redis you can read about.

### One shared layout file, read at startup

**Choice:** every node reads the same `cluster.conf`.

**Why:** you can see the whole cluster in one place, and every node is
guaranteed to agree. Real Redis has nodes discover each other (gossip),
which comes later; doing it in this order keeps redirect bugs and gossip
bugs apart.

**Cost:** changing the layout means editing the file and restarting.

### Find yourself by port

**Choice:** a node finds its own line in the layout by its `--port`.

**Why:** simplest for running several nodes on one machine.

**Cost:** doesn't work across machines, where nodes typically all use the
same port. A `--cluster-node-name` flag is the planned fix.

### Unowned slots fail one at a time

**Choice:** a slot missing from the layout gets `CLUSTERDOWN Hash slot not
served`; every other slot keeps working. `CLUSTER INFO` still reports
`cluster_state:fail`, as Redis would.

**Why:** a mistake in the layout shouldn't take down every key. (Redis's
default is to stop serving everything until all slots are covered.)

### Check before running, never after

**Choice:** the cluster check happens before a command's handler runs.
Out-of-memory checks also run before the handler.

**Why:** a refused command must have no effect: nothing stored, nothing
written to the AOF.

### Transactions are checked twice

**Choice:** each command is checked when it's queued, and all queued keys
are checked together at `EXEC`.

**Why:** that's what Redis does. Checking at queue time tells the client
straight away (and `MOVED` says where to go). Checking at `EXEC` catches
commands that are each fine but use different slots. Together they
guarantee a transaction never runs halfway.

**Why "same slot", not "same node":** slots are what move between nodes.
A transaction that only needed "same node" could break later just because
a slot moved.

### AOF replay skips the cluster check

**Choice:** at startup, commands read back from the AOF run directly, not
through the cluster check.

**Why:** everything in a node's AOF was already accepted by that node. If
the layout changed while the node was down, replaying through the check
would silently drop those keys.

### Node IDs from names

**Choice:** a node's 40-character ID is the SHA-1 of its name.

**Why:** tools expect IDs of that shape, and deriving them from names keeps
them the same across restarts without storing anything.

## Tests start real processes

**Choice:** the end-to-end test starts three real `rediska` processes
instead of three servers inside one test.

**Why:** the store and the cluster layout are package-level variables, so
three nodes in one process would share one store. Separate processes test
exactly what you run. See [Testing](testing.md).
