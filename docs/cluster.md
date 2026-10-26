# Cluster mode

In cluster mode, several Rediska nodes split the keys between them, the way
Redis Cluster does. Each node holds only its share, and clients are sent to
the right node.

!!! info "Current scope"
The cluster layout is **fixed**: every node reads it from a file at
startup. Nodes check on each other over the [cluster bus](#the-cluster-bus),
but they don't share the layout or detect failures yet, slots can't move
while running, and there are no replicas. See [Limitations](#limitations).

## Slots

Every key belongs to one of **16384 hash slots**:

```
slot = CRC16(key) mod 16384
```

Each node owns some of the slots, and stores only the keys in them.

```mermaid
flowchart LR
    K1["name"] --> S1["slot 5798"] --> N2["node2 (5461–10922)"]
    K2["age"] --> S2["slot 741"] --> N1["node1 (0–5460)"]
    K3["visits"] --> S3["slot 12263"] --> N3["node3 (10923–16383)"]
```

There are far more keys than slots, so each slot holds many keys. Slots are
the unit that gets assigned to nodes (and, in a later phase, moved between
them), so the cluster map stays small: 16384 entries, however many keys you
have.

```console
$ redis-cli -p 7001 cluster keyslot name
(integer) 5798
```

### Hash tags

If a key contains `{...}`, only the part inside the braces is hashed. Keys
with the same tag always land in the same slot:

```console
$ redis-cli -p 7001 cluster keyslot {user1}.name
(integer) 8106
$ redis-cli -p 7001 cluster keyslot {user1}.email
(integer) 8106
```

Use hash tags for keys you need to use together in one command or
transaction.

## The layout file

Every node reads the same layout file, which lists every node, its address
and the slots it owns:

```text title="cluster.conf"
# name   address          slots
node1  127.0.0.1:7001  0-5460
node2  127.0.0.1:7002  5461-10922
node3  127.0.0.1:7003  10923-16383
```

- Slots can be ranges (`0-5460`) or single slots (`42`), several per line.
- Anything after `#` is a comment.
- A node finds **its own** line by matching its `--port`.
- Any mistake (overlapping slots, a slot above 16383, a duplicate name or
  address, no line for this node's port) stops the node at startup, naming
  the line.
- Slots that no line lists are _unowned_: commands on them get
  `CLUSTERDOWN`, while every other slot keeps working.

Start each node with the same file and its own port:

```bash
rediska --port 7001 --cluster-enabled --cluster-config-file cluster.conf
rediska --port 7002 --cluster-enabled --cluster-config-file cluster.conf
rediska --port 7003 --cluster-enabled --cluster-config-file cluster.conf
```

Each node also listens on its client port + 10000 (here 17001–17003) for the
[cluster bus](#the-cluster-bus), so those ports must be free too.

### The cluster script

`scripts/cluster.sh` does all of that for you:

```bash
./scripts/cluster.sh start     # build, write the layout, start the nodes
./scripts/cluster.sh status    # which nodes are running, and their key counts
./scripts/cluster.sh stop      # stop them; data stays in each node's AOF
./scripts/cluster.sh clean     # stop and delete all their files
```

Files go in `/tmp/rediska-cluster/`. `NODES=4 ./scripts/cluster.sh start`
starts 4 nodes with the slots split evenly (run `clean` first if you change
the number of nodes).

## How a command is routed

Before running a command, a node checks its keys, in the same order as
Redis:

```mermaid
flowchart TD
    A[command arrives] --> B{any keys?}
    B -- "no (PING, INFO, ...)" --> RUN[run it here]
    B -- yes --> C{all keys in one slot?}
    C -- no --> X1["CROSSSLOT Keys in request<br/>don't hash to the same slot"]
    C -- yes --> D{does anyone own the slot?}
    D -- no --> X2["CLUSTERDOWN Hash slot not served"]
    D -- yes --> E{is it me?}
    E -- no --> X3["MOVED &lt;slot&gt; &lt;host:port&gt;"]
    E -- yes --> RUN
```

The error texts match Redis 7 word for word, so clients that understand
Redis Cluster understand Rediska:

```console
$ redis-cli -p 7001 set name vinay
(error) MOVED 5798 127.0.0.1:7002
$ redis-cli -c -p 7001 set name vinay
-> Redirected to slot [5798] located at 127.0.0.1:7002
OK
$ redis-cli -p 7001 del age name
(error) CROSSSLOT Keys in request don't hash to the same slot
```

`redis-cli -c` follows `MOVED` redirects for you. In a pipeline, each
command gets its own reply, in order.

## The cluster bus

Every node opens a second port for other nodes, at its client port + 10000
(7001 → 17001). Clients never use it.

Over the bus, each node:

- connects to every other node in `cluster.conf`, retrying every second if
  one isn't up yet
- sends each connected node a `PING` once a second, and answers every `PING`
  with a `PONG`
- notices when a connection closes, shows that node as `disconnected`, and
  reconnects by itself when it comes back

Messages use RESP, the same format as client commands. Each one is an array
of 7 items: type, the sender's ID, name, address and flags, the sender's own
slots (as start/end pairs), and a gossip section listing other nodes. You can
talk to the bus by hand:

```console
$ (printf '*7\r\n$4\r\nPING\r\n$1\r\nx\r\n$4\r\ntest\r\n$14\r\n127.0.0.1:9999\r\n:0\r\n*0\r\n*0\r\n'; sleep 1) | nc localhost 17001
```

The reply is a `PONG` carrying node1's ID, name, address and slots
(`0 5460`). Nodes don't act on the slots or gossip sections yet; they're
there for gossip, which comes next.

Bus work runs on the same goroutine as client commands, so cluster state,
like the data, needs no locks. See
[Goroutines in a cluster node](design-decisions.md#goroutines-in-a-cluster-node)
for which goroutine does what.

## Transactions

All keys in a `MULTI`/`EXEC` transaction must be in **one slot**, owned by
the node you're connected to. Rediska checks this in two places, as Redis
does:

**While queuing**, each command is checked on its own. If one fails, the
transaction is marked broken and `EXEC` refuses to run anything:

```console
127.0.0.1:7001> multi
OK
127.0.0.1:7001(TX)> set age 30
QUEUED
127.0.0.1:7001(TX)> set name vinay
(error) MOVED 5798 127.0.0.1:7002
127.0.0.1:7001(TX)> exec
(error) EXECABORT Transaction discarded because of previous errors.
```

**At `EXEC`**, all the queued keys are checked together. This catches
commands that are fine on their own but use different slots:

```console
127.0.0.1:7001> multi
OK
127.0.0.1:7001(TX)> set age 30
QUEUED
127.0.0.1:7001(TX)> set foo2 x
QUEUED
127.0.0.1:7001(TX)> exec
(error) CROSSSLOT Keys in request don't hash to the same slot
```

`age` (slot 741) and `foo2` (slot 1044) are both on node1, but in different
slots, so the transaction is refused. Either way, a transaction never runs
halfway. Use hash tags to keep a transaction's keys together:

```console
127.0.0.1:7002> multi
OK
127.0.0.1:7002(TX)> set {user1}.name vinay
QUEUED
127.0.0.1:7002(TX)> incr {user1}.visits
QUEUED
127.0.0.1:7002(TX)> exec
1) OK
2) (integer) 1
```

## CLUSTER commands

| Command                            | Returns                                                    | Used by                                            |
| ---------------------------------- | ---------------------------------------------------------- | -------------------------------------------------- |
| `CLUSTER KEYSLOT key`              | the key's slot                                             | debugging                                          |
| `CLUSTER COUNTKEYSINSLOT slot`     | how many of **this node's** keys are in the slot           | checking how data is spread; moving slots          |
| `CLUSTER GETKEYSINSLOT slot count` | up to `count` of this node's keys in the slot              | moving slots                                       |
| `CLUSTER SLOTS`                    | slot ranges and their owners                               | client libraries, to learn the map                 |
| `CLUSTER NODES`                    | one line per node                                          | `redis-cli --cluster`, `redis-benchmark --cluster` |
| `CLUSTER MYID`                     | this node's 40-character ID                                | debugging                                          |
| `CLUSTER INFO`                     | `cluster_state:ok` if every slot has an owner, plus counts | monitoring                                         |

`INFO cluster` reports `cluster_enabled:1` on a cluster node.

`COUNTKEYSINSLOT` and `GETKEYSINSLOT` answer about the node you ask, whoever
owns the slot, and never redirect:

```console
$ redis-cli -p 7002 cluster countkeysinslot 8106
(integer) 3
$ redis-cli -p 7002 cluster getkeysinslot 8106 10
1) "{user1}.name"
2) "{user1}.email"
3) "{user1}.visits"
$ redis-cli -p 7001 cluster countkeysinslot 8106
(integer) 0
```

To answer these quickly, each cluster node keeps a small index of which of
its keys are in each slot, next to its data. It's updated whenever a key is
created or removed (including by expiry and eviction), and it's switched off
on a normal single node.

Node IDs are derived from the node's name, so they stay the same across
restarts.

In `CLUSTER NODES`, the ping-sent and pong-received columns come from the
[cluster bus](#the-cluster-bus). They are Unix times in milliseconds (`0`
means none pending, or never), and the link state is `connected` or
`disconnected`. Run it twice: on a healthy cluster, the pong times move
forward every second.

### Tools that work

- `redis-cli -c`: follows redirects.
- `redis-cli --cluster check 127.0.0.1:7001`: reports all 16384 slots
  covered.
- `redis-benchmark --cluster -p 7001`: spreads load across the nodes.

Some client libraries also need commands Rediska doesn't have yet. For
example, redis-py's cluster client reads the slot map, then calls `COMMAND`.

## Persistence in a cluster

Each node has its own AOF, holding only its own keys. When a node restarts,
it loads its whole AOF back **without** the cluster check, so its data is
never dropped, even if the layout file changed while it was down.

## Limitations

- **Fixed layout, no gossip yet**: every node learns the layout from
  `cluster.conf` and connects to every node in it. There's no
  `CLUSTER MEET`; to change the layout, edit the file and restart the nodes.
- **No failure detection yet**: a stopped node shows as `disconnected`,
  but nodes don't yet agree that it has failed, and routing doesn't change.
- **No slot migration**: slots can't move between running nodes.
- **No replicas or failover**: if a node goes down, its slots are
  unavailable until it comes back.
