package cluster

import (
	"strings"
	"testing"
)

// Replica roles on the bus (PR 1). Like failure_test.go, no network: the
// bus's functions are called the way dispatchWorker would.

// fromReplica builds a PING from the named node, saying it's a replica of
// master, with gossip about other nodes' health.
func (f *gossipFixture) fromReplica(name, master string, slots []SlotRange, gossip ...GossipEntry) Message {
	m := f.from("PING", name, slots, gossip...)
	m.Master = f.node(master).ID
	return m
}

// deliver hands m to the bus, and throws away our PONG.
func (f *gossipFixture) deliver(m Message) {
	f.b.handle(m, f.inbox)
	<-f.inbox.out
}

func TestRoleLearnedFromTheNodeItself(t *testing.T) {
	f := newGossipFixture(t) // we are node1; node2 owns 100-199
	node2 := f.node("node2")

	// node2 says it's a replica of node1. A replica owns no slots, even
	// if it announces some.
	f.deliver(f.fromReplica("node2", "node1", []SlotRange{r(100, 199)}))
	if node2.MasterID != f.node("node1").ID {
		t.Fatalf("node2's MasterID = %q, want node1's ID", node2.MasterID)
	}
	f.wantOwner(150, "")

	// It says it's a master again: it gets its slots back.
	f.deliver(f.from("PING", "node2", []SlotRange{r(100, 199)}))
	if node2.IsReplica() {
		t.Fatal("node2 is still a replica after announcing it's a master")
	}
	f.wantOwner(150, "node2")
}

// Gossip says who exists, never what they are: a gossip entry can't make
// a node a replica.
func TestGossipDoesntCarryRoles(t *testing.T) {
	f := newGossipFixture(t)
	f.deliver(f.from("PING", "node2", f.node("node2").Slots,
		GossipEntry{ID: "id-9", Name: "node9", Addr: "127.0.0.1:7009"}))
	if p := f.b.Peer("id-9"); p == nil || p.Node.IsReplica() {
		t.Fatalf("node9 learned by gossip: %+v, want a known master", p)
	}
}

// Only masters' failure reports count toward FAIL.
func TestReplicaReportsDontCount(t *testing.T) {
	f := newFailureFixture(t) // we are node1, a master
	n3 := f.node("node3")
	pfail := GossipEntry{ID: n3.ID, Name: n3.Name, Addr: n3.Addr, Flags: FlagPFail}

	// node2 becomes a replica, which leaves 2 masters with slots: us and
	// node3. FAIL needs both of us... but node3 is the one failing, so it
	// needs us plus a report from a master, and there's none.
	f.deliver(f.fromReplica("node2", "node1", nil))
	node3 := f.b.Peer(n3.ID)
	f.suspect(node3)
	f.deliver(f.fromReplica("node2", "node1", nil, pfail))
	wantHealth(t, node3, true, false) // node2's report was ignored

	if n := len(node3.failReports); n != 0 {
		t.Fatalf("node3 has %d failure reports, want none from a replica", n)
	}
}

// replicaFixture is 3 masters (node1, node2, node3) and node4, a replica
// of node1, as seen by node4.
func newReplicaFixture(t *testing.T) *gossipFixture {
	t.Helper()
	l, err := ParseLayout(strings.NewReader(`
node1 127.0.0.1:7001 0-99
node2 127.0.0.1:7002 100-199
node3 127.0.0.1:7003 300-399
node4 127.0.0.1:7004 replicaof node1
`), 7004)
	if err != nil {
		t.Fatal(err)
	}
	b := NewBus(l)
	b.NodeTimeout = testTimeout
	for _, p := range b.peers {
		p.link, p.Handshake = testLink(), false
	}
	return &gossipFixture{t: t, b: b, l: l, inbox: testLink()}
}

// A replica's own suspicion isn't a master's vote: with 3 masters, a
// replica needs reports from 2 of them before it marks a node FAIL.
func TestReplicaOwnSuspicionDoesntCount(t *testing.T) {
	f := newReplicaFixture(t)
	node3 := f.b.Peer(f.node("node3").ID)

	f.suspect(node3)
	f.report("node2", "node3", FlagPFail)
	wantHealth(t, node3, true, false) // node2 alone is 1 of 2 needed

	f.report("node1", "node3", FlagPFail)
	wantHealth(t, node3, false, true) // node1 and node2: a majority
}

// What a replica sends must say whose replica it is, and claim no slots.
func TestReplicaAnnouncesItsMaster(t *testing.T) {
	f := newReplicaFixture(t)
	m := f.b.msg("PING", f.node("node1").ID)
	if m.Master != f.node("node1").ID {
		t.Fatalf("Master = %q, want node1's ID", m.Master)
	}
	if len(m.Slots) != 0 {
		t.Fatalf("a replica announced slots %v", m.Slots)
	}
}
