# Rediska

Rediska is a Redis-compatible in-memory key-value server written in Go, built
to learn how Redis works on the inside. It speaks RESP, Redis's wire protocol,
so `redis-cli`, `redis-benchmark` and Redis client libraries can talk to it.

```console
$ rediska --port 7379
$ redis-cli -p 7379 set name vinay
OK
$ redis-cli -p 7379 get name
"vinay"
```

## What it does

- **Strings and counters**: `SET` (with `EX`/`PX`), `GET`, `APPEND`, `INCR`,
  `DEL`, `EXPIRE`, `TTL`, stored in Redis's compact encodings (`int`,
  `embstr`, `raw`).
- **HyperLogLog**: `PFADD`, `PFCOUNT`, `PFMERGE`, with Redis's sparse and
  dense representations.
- **Transactions**: `MULTI`, `EXEC`, `DISCARD`.
- **Persistence**: an append-only file (AOF), replayed at startup.
- **Expiry**: lazy expiry on access, plus an active expiry loop.
- **Memory limits**: `--maxmemory` with `noeviction`, approximate LRU or LFU
  eviction.
- **Cluster mode**: 16384 hash slots, `MOVED` and `CROSSSLOT` redirects, and
  the `CLUSTER` commands real cluster tools use.
  See [Cluster mode](cluster.md).
- **Redis's threading model**: one goroutine per connection, but every
  command runs on a single dispatch worker, so data needs no locks.
  See [Design decisions](design-decisions.md).

## Where to go next

| I want to... | Read |
|---|---|
| install Rediska and run it | [Getting started](getting-started.md) |
| run a multi-node cluster | [Cluster mode](cluster.md) |
| understand why it's built this way | [Design decisions](design-decisions.md) |
| run or write tests | [Testing](testing.md) |
| read the Go API | [API reference](api/cluster.md) |
| see how it got here | [Changelog](changelog.md) |
