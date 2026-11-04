package commands

import (
	"bufio"
	"context"
	"fmt"
	"math/rand/v2"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/vinayhpandya/rediska/cluster"
	"github.com/vinayhpandya/rediska/core"
	"github.com/vinayhpandya/rediska/replication"
	"github.com/vinayhpandya/rediska/store"
)

// clusterStore gives the test its own store.Default with the slot index
// on, as in cluster mode, and returns it.
func clusterStore(t *testing.T) *store.Store {
	t.Helper()
	old := store.Default
	s := store.New()
	s.EnableSlotIndex()
	store.Default = s
	t.Cleanup(func() { store.Default = old })
	return s
}

// The snapshot of every slot, replayed into an empty store, recreates
// the data exactly: values, types and expiry times.
func TestSnapshotRecreatesTheData(t *testing.T) {
	master := clusterStore(t)
	do := func(args ...string) {
		t.Helper()
		if r := Dispatch(&Command{Name: args[0], Args: args[1:]}); isErrorReply(r) {
			t.Fatalf("%v: %s", args, r)
		}
	}
	do("SET", "int", "42")
	do("SET", "short", "hello")
	do("SET", "long", fmt.Sprintf("%050d", 7))
	do("APPEND", "appended", "abc")
	do("SET", "ttl", "v", "EX", "100")
	do("PFADD", "small-hll", "a", "b", "c")
	for i := range 3000 { // enough to turn this one dense
		do("PFADD", "big-hll", fmt.Sprint(i))
	}
	do("SET", "expired", "v", "PX", "1")
	time.Sleep(5 * time.Millisecond)

	replica := store.New()
	replica.EnableSlotIndex()
	var cmds [][]string
	for slot := range cluster.NumSlots {
		cmds = append(cmds, master.SlotCommands(slot)...)
	}
	store.Default = replica
	for _, c := range cmds {
		Replay(&Command{Name: c[0], Args: c[1:]})
	}
	store.Default = master

	if master.Digest() != replica.Digest() {
		t.Fatal("the replayed snapshot differs from the original")
	}
	if replica.KeyCount() != 7 { // everything but "expired"
		t.Fatalf("replica has %d keys, want 7", replica.KeyCount())
	}
	if got, want := replica.TTL("ttl"), master.TTL("ttl"); got != want {
		t.Fatalf("TTL after copying = %d, want %d", got, want)
	}
	if n, _ := replica.PFCount("big-hll"); n < 2900 || n > 3100 {
		t.Fatalf("PFCOUNT big-hll = %d after copying, want about 3000", n)
	}
}

// The heart of replication: a replica copies a master's data while
// thousands of random writes happen, and ends up with exactly the same
// data. Copying, writes and the replica's applying all take turns on one
// "worker" goroutine, as they do in the server.
func TestReplicaMatchesMasterDespiteWritesDuringCopy(t *testing.T) {
	master := clusterStore(t)
	m := freshMaster(t)
	replicaStore := store.New()
	replicaStore.EnableSlotIndex()

	oldBatch := replication.BatchKeys
	replication.BatchKeys = 50 // copy in many small steps, so writes land between them
	t.Cleanup(func() { replication.BatchKeys = oldBatch })

	// The worker: every task runs with the master's store, except the
	// replica's applying, which switches to the replica's store for its turn.
	tasks := make(chan func())
	go func() {
		for f := range tasks {
			store.Default = master
			f()
		}
	}()
	run := func(f func()) bool {
		done := make(chan struct{})
		tasks <- func() { f(); close(done) }
		<-done
		return true
	}
	do := func(args ...string) {
		run(func() { Dispatch(&Command{Name: args[0], Args: args[1:]}) })
	}

	for i := range 5000 {
		do("SET", fmt.Sprintf("key:%d", i), fmt.Sprintf("v%d", i))
	}

	// The master serves the replica over an in-memory connection. When
	// the test ends, both sides stop before the worker does: the master
	// uses the worker to forget the replica.
	ctx, cancel := context.WithCancel(context.Background())
	masterEnd, replicaEnd := net.Pipe()
	var sides sync.WaitGroup
	sides.Add(2)
	t.Cleanup(func() {
		cancel()
		replicaEnd.Close()
		sides.Wait()
		close(tasks)
	})
	go func() {
		defer sides.Done()
		m.Serve(ctx, masterEnd, run, "", -1) // a new replica: PSYNC ? -1
	}()

	// The replica: apply everything the master sends, in order.
	go func() {
		defer sides.Done()
		r := bufio.NewReader(replicaEnd)
		if v, err := core.Decode(r); err != nil || v != "FULLRESYNC" {
			return
		}
		var block [][]string
		inBlock := false
		for {
			v, err := core.Decode(r)
			if err != nil {
				return
			}
			arr := v.([]any)
			args := make([]string, len(arr))
			for i, x := range arr {
				args[i] = x.(string)
			}
			switch {
			case args[0] == "MULTI":
				inBlock, block = true, nil
				continue
			case args[0] == "EXEC":
				inBlock = false
			case inBlock:
				block = append(block, args)
				continue
			default:
				block = [][]string{args}
			}
			cmds := block
			run(func() {
				store.Default = replicaStore
				for _, c := range cmds {
					Replay(&Command{Name: c[0], Args: c[1:]})
				}
			})
		}
	}()

	// Random writes while the copy runs: plain commands, and transactions
	// on keys that share a slot.
	rng := rand.New(rand.NewPCG(1, 2))
	for i := range 3000 {
		k := fmt.Sprintf("key:%d", rng.IntN(6000))
		switch rng.IntN(7) {
		case 0:
			do("SET", k, fmt.Sprintf("new%d", i))
		case 1:
			do("DEL", k)
		case 2:
			do("INCR", fmt.Sprintf("counter:%d", rng.IntN(100)))
		case 3:
			do("APPEND", k, "+")
		case 4:
			do("SET", k, "ttl", "EX", "1000")
		case 5:
			do("PFADD", fmt.Sprintf("hll:%d", rng.IntN(20)), fmt.Sprint(i))
		case 6:
			tag := fmt.Sprintf("{user%d}", rng.IntN(50))
			run(func() {
				DispatchBatch([]*Command{
					{Name: "SET", Args: []string{tag + ".name", fmt.Sprint(i)}},
					{Name: "INCR", Args: []string{tag + ".visits"}},
				})
			})
		}
	}

	// Once the replica has caught up, both stores must hold the same data.
	deadline := time.Now().Add(10 * time.Second)
	for {
		var same bool
		var masterKeys, replicaKeys int
		run(func() {
			same = master.Digest() == replicaStore.Digest()
			masterKeys, replicaKeys = master.KeyCount(), replicaStore.KeyCount()
		})
		if same && masterKeys == replicaKeys {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("the replica never caught up: master has %d keys, replica %d, digests differ: %v",
				masterKeys, replicaKeys, !same)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// freshMaster gives the test its own replication.Default, so earlier
// tests' backlog and counters don't leak in.
func freshMaster(t *testing.T) *replication.Master {
	t.Helper()
	old := replication.Default
	m := replication.NewMaster()
	m.OnChange = updateTracking
	replication.Default = m
	t.Cleanup(func() { replication.Default = old })
	return m
}

// A replica whose connection drops while writes keep coming carries on
// from where it was, without copying everything again, and still ends up
// with exactly the master's data. This uses the real replica side
// (replication.Link) over TCP.
func TestReplicaResumesAfterDisconnects(t *testing.T) {
	master := clusterStore(t)
	replicaStore := store.New()
	replicaStore.EnableSlotIndex()
	m := freshMaster(t)
	oldRetry := replication.RetryInterval
	replication.RetryInterval = 20 * time.Millisecond
	t.Cleanup(func() { replication.RetryInterval = oldRetry })

	// The worker, as in the test above.
	tasks := make(chan func(), 1024)
	go func() {
		for f := range tasks {
			store.Default = master
			f()
		}
	}()
	run := func(f func()) bool {
		done := make(chan struct{})
		tasks <- func() { f(); close(done) }
		<-done
		return true
	}
	do := func(args ...string) {
		run(func() { Dispatch(&Command{Name: args[0], Args: args[1:]}) })
	}
	for i := range 2000 {
		do("SET", fmt.Sprintf("key:%d", i), "v")
	}

	// The master: accept replicas and serve their PSYNC.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				v, err := core.Decode(bufio.NewReader(conn))
				if err != nil {
					conn.Close()
					return
				}
				arr := v.([]any)
				replID, offset, err := replication.ParsePSync([]string{arr[1].(string), arr[2].(string)})
				if err != nil {
					conn.Close()
					return
				}
				m.Serve(ctx, conn, run, replID, offset)
			}()
		}
	}()

	// The replica: the real Link, applying into its own store.
	link := replication.NewLink(ln.Addr().String())
	submit := func(f func()) bool { tasks <- f; return true }
	wg.Add(1)
	go func() {
		defer wg.Done()
		link.Run(ctx,
			func() bool { return submit(func() { replicaStore.Flush() }) },
			func(cmds [][]string) bool {
				return submit(func() {
					store.Default = replicaStore
					for _, c := range cmds {
						Replay(&Command{Name: c[0], Args: c[1:]})
					}
				})
			})
	}()
	t.Cleanup(func() {
		cancel()
		ln.Close()
		link.Reconnect()
		wg.Wait()
		close(tasks)
	})

	waitFor := func(what string, ok func() bool) {
		t.Helper()
		deadline := time.Now().Add(10 * time.Second)
		for {
			var done bool
			run(func() { done = ok() })
			if done {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("timed out waiting for %s", what)
			}
			time.Sleep(5 * time.Millisecond)
		}
	}
	inSync := func() bool {
		return master.Digest() == replicaStore.Digest() && master.KeyCount() == replicaStore.KeyCount() &&
			link.Offset() == m.Offset()
	}
	waitFor("the first copy", func() bool { return link.Offset() >= 0 && inSync() })

	// Drop the connection 5 times, with writes before, during and after.
	rng := rand.New(rand.NewPCG(3, 4))
	write := func(i int) {
		k := fmt.Sprintf("key:%d", rng.IntN(3000))
		switch rng.IntN(4) {
		case 0:
			do("SET", k, fmt.Sprint(i))
		case 1:
			do("DEL", k)
		case 2:
			do("INCR", fmt.Sprintf("counter:%d", rng.IntN(20)))
		case 3:
			tag := fmt.Sprintf("{user%d}", rng.IntN(20))
			run(func() {
				DispatchBatch([]*Command{
					{Name: "SET", Args: []string{tag + ".a", fmt.Sprint(i)}},
					{Name: "INCR", Args: []string{tag + ".b"}},
				})
			})
		}
	}
	for round := range 5 {
		for i := range 200 {
			write(round*1000 + i)
		}
		link.Reconnect()
		for i := range 200 { // most of these happen while it's disconnected
			write(round*1000 + 500 + i)
		}
		waitFor(fmt.Sprintf("the replica to catch up after disconnect %d", round+1), inSync)
	}

	var full, partial int
	run(func() { full, partial = m.FullSyncs(), m.PartialSyncs() })
	if full != 1 || partial != 5 {
		t.Fatalf("%d full and %d partial resyncs; want 1 full (the first copy) and 5 partial", full, partial)
	}
}
