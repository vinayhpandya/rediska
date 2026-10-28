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
//
// Each test node gets its own copy of the nodes, as separate processes
// would: the bus changes them (a peer's slots, say) on its own worker.
func startTestNode(t *testing.T, nodes []*Node, self *Node) *testNode {
	t.Helper()
	l := &Layout{}
	for _, n := range nodes {
		c := *n
		c.Slots = append([]SlotRange(nil), n.Slots...)
		l.Nodes = append(l.Nodes, &c)
		if n == self {
			l.Self = &c
		}
	}
	b := NewBus(l)
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

// Three nodes that each know only themselves. node2 and node3 are
// introduced only to node1; gossip has to tell them about each other, and
// each node has to learn the others' slots from the others themselves.
func TestMeetSpreadsByGossip(t *testing.T) {
	// Client ports 27011-27013, so the bus uses 37011-37013.
	ranges := []SlotRange{{0, 5460}, {5461, 10922}, {10923, 16383}}
	var running []*testNode
	for i := 1; i <= 3; i++ {
		port := 27010 + i
		self := &Node{
			Name:  fmt.Sprintf("node%d", i),
			Addr:  fmt.Sprintf("127.0.0.1:%d", port),
			Port:  port,
			ID:    fmt.Sprintf("meet-node-%d", i),
			Slots: []SlotRange{ranges[i-1]},
		}
		running = append(running, startTestNode(t, []*Node{self}, self))
	}
	defer func() {
		for _, n := range running {
			n.stop(t)
		}
	}()
	// startTestNode doesn't fill owner[]; give each node its own slots.
	for _, n := range running {
		n.onWorker(func() { setOwnSlots(n.bus.layout) })
	}

	for _, n := range running[1:] {
		n.onWorker(func() {
			if err := n.bus.Meet("127.0.0.1", 27011); err != nil {
				t.Error(err)
			}
		})
	}

	for _, n := range running {
		n.waitFor(t, "it knows both other nodes, trusts them, and has every slot", func(b *Bus) bool {
			if len(b.Peers()) != 2 || !allUp(b) {
				return false
			}
			for _, p := range b.Peers() {
				if p.Handshake {
					return false
				}
			}
			for s := 0; s < NumSlots; s++ {
				if b.layout.Owner(s) == nil {
					return false
				}
			}
			return true
		})
	}
	// And the slots went to the right nodes.
	running[1].onWorker(func() {
		for slot, want := range map[int]string{0: "node1", 8000: "node2", 16383: "node3"} {
			if got := running[1].bus.layout.Owner(slot); got == nil || got.Name != want {
				t.Errorf("node2 thinks slot %d belongs to %v, want %s", slot, got, want)
			}
		}
	})
}

// A MEET to an address where nobody answers is forgotten after the
// handshake timeout, so it doesn't stay in CLUSTER NODES forever.
func TestMeetDeadAddressIsForgotten(t *testing.T) {
	old := handshakeTimeout
	handshakeTimeout = 500 * time.Millisecond
	defer func() { handshakeTimeout = old }()

	self := &Node{Name: "node1", Addr: "127.0.0.1:27021", Port: 27021, ID: "dead-test-1"}
	n := startTestNode(t, []*Node{self}, self)
	defer n.stop(t)

	n.onWorker(func() { n.bus.Meet("127.0.0.1", 27029) }) // nothing listens on 37029
	n.waitFor(t, "the dead address is in the peer list", func(b *Bus) bool { return len(b.Peers()) == 1 })
	n.waitFor(t, "the dead address is forgotten", func(b *Bus) bool {
		return len(b.Peers()) == 0 && len(b.layout.Nodes) == 1
	})
}

// setOwnSlots fills the owner table from Self's slots, as ParseLayout would.
func setOwnSlots(l *Layout) {
	for _, r := range l.Self.Slots {
		for s := r.Start; s <= r.End; s++ {
			l.owner[s] = l.Self
		}
	}
}
