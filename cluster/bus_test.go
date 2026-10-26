package cluster

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// testNode is one node of an in-process test cluster: a bus plus a small
// stand-in for the server's dispatch worker.
type testNode struct {
	bus    *Bus
	jobs   chan func() // the stand-in for commandCh
	cancel context.CancelFunc
}

// startTestNode starts a bus for self, with its own worker goroutine that
// runs every event through Handle, one at a time, like dispatchWorker.
func startTestNode(t *testing.T, nodes []*Node, self *Node) *testNode {
	t.Helper()
	b := NewBus(&Layout{Nodes: nodes, Self: self})
	if err := b.Listen("127.0.0.1"); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	n := &testNode{bus: b, jobs: make(chan func(), 64), cancel: cancel}
	go func() {
		for f := range n.jobs {
			f()
		}
	}()
	b.Start(ctx, func(e Event) {
		select {
		case n.jobs <- func() { b.Handle(e) }:
		case <-ctx.Done():
		}
	})
	return n
}

// stop shuts the node down and waits for every bus goroutine to finish.
func (n *testNode) stop(t *testing.T) {
	t.Helper()
	n.cancel()
	done := make(chan struct{})
	go func() { n.bus.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatalf("%s: bus did not shut down", n.bus.layout.Self.Name)
	}
}

// onWorker runs f on the node's worker, where reading peers is safe, and
// waits for it.
func (n *testNode) onWorker(f func()) {
	done := make(chan struct{})
	n.jobs <- func() { f(); close(done) }
	<-done
}

// waitFor checks cond on the node's worker every 100 ms until it's true,
// or fails after 5 seconds.
func (n *testNode) waitFor(t *testing.T, what string, cond func(b *Bus) bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var ok bool
		n.onWorker(func() { ok = cond(n.bus) })
		if ok {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("%s: timed out waiting until %s", n.bus.layout.Self.Name, what)
}

func allUp(b *Bus) bool {
	for _, p := range b.Peers() {
		if !p.Connected() || p.PongRecv.IsZero() {
			return false
		}
	}
	return true
}

// Three nodes connect and PING each other; when one stops, the others see
// it disconnected, and when it comes back they reconnect by themselves.
func TestBusConnectsAndReconnects(t *testing.T) {
	// Client ports 27001-27003, so the bus uses 37001-37003.
	var nodes []*Node
	for i := 1; i <= 3; i++ {
		port := 27000 + i
		nodes = append(nodes, &Node{
			Name: fmt.Sprintf("node%d", i),
			Addr: fmt.Sprintf("127.0.0.1:%d", port),
			Port: port,
			ID:   fmt.Sprintf("test-node-%d", i),
		})
	}

	var running []*testNode
	for _, self := range nodes {
		running = append(running, startTestNode(t, nodes, self))
	}
	node1, node3 := running[0], running[2]

	for _, n := range running {
		n.waitFor(t, "every peer is connected and has answered a PING", allUp)
	}

	running[1].stop(t)
	node1.waitFor(t, "node2 is disconnected", func(b *Bus) bool {
		return !b.Peer("test-node-2").Connected()
	})

	running[1] = startTestNode(t, nodes, nodes[1])
	for _, n := range running {
		n.waitFor(t, "every peer is connected again", allUp)
	}

	for _, n := range []*testNode{node1, running[1], node3} {
		n.stop(t)
	}
}
