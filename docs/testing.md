# Testing

## Running the tests

```bash
go test ./...            # everything, including the 3-node end-to-end test
go test -short ./...     # fast unit tests only
go test -race ./...      # with Go's race detector, as CI runs them
```

To run one package or one test:

```bash
go test ./cluster/
go test ./commands/ -run TestCheckCluster -v
```

## What's tested where

| Package | Tests |
|---|---|
| `cluster` | key slots (through the examples), parsing `cluster.conf` (including 9 kinds of broken file), slot ownership |
| `commands` | routing (`MOVED`, `CROSSSLOT`, `CLUSTERDOWN`, hash tags), `CLUSTER` replies, AOF replay, out-of-memory, `PFADD` errors |
| `server` | `MULTI`/`EXEC` in cluster mode, with the replies real Redis gives |
| `store` | `PFADD` key counts and memory accounting |
| `core` | RESP parsing |
| `e2e` | a real 3-node cluster, over TCP |

## The end-to-end test

`e2e/cluster_test.go` checks the whole cluster the way a client sees it:

1. builds `rediska` into a temporary folder,
2. picks three free ports and writes a layout for them,
3. starts three real `rediska` processes,
4. sends commands over TCP and checks every reply: redirects, hash tags,
   pipelines, transactions and `CLUSTER SLOTS`,
5. stops the nodes and deletes everything, even if a check fails.

It takes a couple of seconds, mostly for the build, and `go test -short`
skips it.

## Examples are tests too

Functions named `Example...` in `_test.go` files appear in the API docs as
usage examples, and `go test` runs them and checks their output against the
`// Output:` comment. For example, from `cluster/example_test.go`:

```go
func ExampleKeyHashSlot() {
	fmt.Println(cluster.KeyHashSlot("name"))
	fmt.Println(cluster.KeyHashSlot("{user1}.name"), cluster.KeyHashSlot("{user1}.email"))
	// Output:
	// 5798
	// 8106 8106
}
```

If the code changes so the output no longer matches, the test fails, so the
examples in the docs can't go out of date.

## Comparing with real Redis

Many expected replies in the tests were first recorded from a real Redis 7
server, so Rediska's behaviour matches it word for word. To compare by hand,
run a real Redis next to Rediska:

```bash
redis-server --port 6390 --cluster-enabled yes --cluster-config-file /tmp/nodes-6390.conf
for k in name age visits "{user1}.name"; do
  echo "$k  redis=$(redis-cli -p 6390 cluster keyslot "$k")  rediska=$(redis-cli -p 7001 cluster keyslot "$k")"
done
```

## Continuous integration

Every push to `main` and every pull request runs, on Linux and macOS:

1. a `gofmt` check (fails if any file isn't formatted),
2. `go vet ./...`,
3. `go build ./...`,
4. `go test -race ./...`.

See `.github/workflows/ci.yaml`. A separate workflow builds and publishes
this documentation site.
