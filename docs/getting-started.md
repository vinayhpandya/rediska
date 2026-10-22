# Getting started

## Install

You need [Go](https://go.dev/dl/) 1.25 or newer.

```bash
go install github.com/vinayhpandya/rediska@latest
```

This builds the `rediska` binary into `$(go env GOPATH)/bin`, usually
`~/go/bin`. If your shell can't find `rediska` afterwards, add that folder to
your `PATH`, for example in `~/.zshrc`:

```bash
export PATH="$PATH:$(go env GOPATH)/bin"
```

!!! note
    `@latest` installs the newest tagged release. To try the newest code on
    the `main` branch instead, use `@main`.

### From source

```bash
git clone https://github.com/vinayhpandya/rediska
cd rediska
go build -o rediska .
./rediska
```

You'll also want `redis-cli` to talk to it. On macOS: `brew install redis`.

## Run a single server

```bash
rediska --port 7379
```

In another terminal:

```console
$ redis-cli -p 7379 ping
PONG
$ redis-cli -p 7379 set name vinay EX 60
OK
$ redis-cli -p 7379 ttl name
(integer) 60
$ redis-cli -p 7379 incr visits
(integer) 1
$ redis-cli -p 7379 object encoding visits
"int"
```

Stop the server with ++ctrl+c++. It shuts down cleanly: it finishes the
commands already in flight and flushes the AOF to disk first.

## Keep data across restarts

By default, data lives only in memory. To keep it, turn on the append-only
file (AOF):

```bash
rediska --port 7379 --appendOnly --appendOnlyFile data.aof
```

Every successful write command is appended to `data.aof`, and the file is
replayed when the server starts.

## Limit memory

```bash
rediska --port 7379 --maxmemory 100 --maxmemory-policy allkeys-lru
```

`--maxmemory` is in megabytes. When the limit is reached:

| Policy | What happens |
|---|---|
| `noeviction` (default) | writes are refused with an `OOM` error; reads and `DEL` still work |
| `allkeys-lru` | the least recently used keys are evicted (approximate, by sampling) |
| `allkeys-lfu` | the least frequently used keys are evicted |
| `volatile-lfu` | currently the same as `allkeys-lfu` (Redis would only evict keys that have an expiry) |

## All flags

| Flag | Default | Meaning |
|---|---|---|
| `--host` | `0.0.0.0` | address to listen on (`0.0.0.0` = every network interface) |
| `--port` | `7379` | TCP port |
| `--appendOnly` | off | turn on the append-only file |
| `--appendOnlyFile` | | path of the append-only file |
| `--maxmemory` | `0` | memory limit in MB (`0` = no limit) |
| `--maxmemory-policy` | `noeviction` | what to do at the limit (see above) |
| `--maxmemory-samples` | `5` | keys sampled per eviction round |
| `--lfu-log-factor` | `10` | how slowly LFU counters grow (higher = slower) |
| `--lfu-decay-time` | `1` | idle minutes per LFU counter decay step |
| `--cluster-enabled` | off | run as a cluster node |
| `--cluster-config-file` | | cluster layout file (required with `--cluster-enabled`) |
| `--pprof-addr` | off | serve Go's profiler, e.g. `localhost:6060` |

## Run a cluster

The repository includes a script that starts a 3-node cluster on your
machine:

```bash
./scripts/cluster.sh start
redis-cli -c -p 7001 set name vinay   # -c follows MOVED redirects
./scripts/cluster.sh stop
```

See [Cluster mode](cluster.md) for how it works.

## Supported commands

`APPEND`, `CLUSTER`, `DEBUG`, `DEL`, `DISCARD`, `EXEC`, `EXPIRE`, `GET`,
`INCR`, `INFO`, `MULTI`, `OBJECT`, `PFADD`, `PFCOUNT`, `PFMERGE`, `PING`,
`SET`, `TTL`.
