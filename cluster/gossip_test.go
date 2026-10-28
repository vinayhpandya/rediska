package cluster

import (
	"bufio"
	"bytes"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/vinayhpandya/rediska/core"
)

// gossipFixture is a bus for node1 in a 3-node layout, with no network:
// tests feed it messages through handle, as dispatchWorker would.
//
// node2 and node3 come from cluster.conf, so they start in handshake. The
// fixture finishes their handshakes, as if each had answered on its own
// link, so tests start from a settled cluster.
type gossipFixture struct {
	t     *testing.T
	b     *Bus
	l     *Layout
	inbox *link // an incoming link: messages arrive here, and replies go here
}

func newGossipFixture(t *testing.T) *gossipFixture {
	t.Helper()
	l := smallLayout(t) // config_test.go
	b := NewBus(l)
	for _, p := range b.peers {
		p.link, p.Handshake = testLink(), false
	}
	return &gossipFixture{t: t, b: b, l: l, inbox: testLink()}
}

// testLink is a link with no socket: what's sent to it waits in out.
func testLink() *link {
	return &link{out: make(chan []byte, 64), closed: make(chan struct{})}
}

func (f *gossipFixture) node(name string) *Node {
	for _, n := range f.l.Nodes {
		if n.Name == name {
			return n
		}
	}
	f.t.Fatalf("no node %q", name)
	return nil
}

// from builds a message from the named node, announcing slots.
func (f *gossipFixture) from(typ, name string, slots []SlotRange, gossip ...GossipEntry) Message {
	n := f.node(name)
	return Message{Type: typ, ID: n.ID, Name: n.Name, Addr: n.Addr, Slots: slots, Gossip: gossip}
}

func (f *gossipFixture) wantOwner(slot int, name string) {
	f.t.Helper()
	if got := ownerName(f.l, slot); got != name {
		f.t.Fatalf("slot %d: owner %q, want %q", slot, got, name)
	}
}

func r(start, end int) SlotRange { return SlotRange{start, end} }

func TestPeerSlotsUpdateRouting(t *testing.T) {
	f := newGossipFixture(t)
	node2 := f.node("node2")

	// node2 takes unowned slots 200-299 as well as its own.
	f.b.handle(f.from("PING", "node2", []SlotRange{r(100, 299)}), f.inbox)
	f.wantOwner(250, "node2")
	if want := []SlotRange{r(100, 299)}; !reflect.DeepEqual(node2.Slots, want) {
		t.Fatalf("node2.Slots = %v, want %v", node2.Slots, want)
	}

	// It drops 150-299: those slots are released.
	f.b.handle(f.from("PING", "node2", []SlotRange{r(100, 149)}), f.inbox)
	f.wantOwner(120, "node2")
	f.wantOwner(180, "")
	f.wantOwner(250, "")

	// Announcing nothing releases everything, even on the very first message.
	f.b.handle(f.from("PING", "node3", nil), f.inbox)
	f.wantOwner(350, "")
}

func TestFirstOwnerWins(t *testing.T) {
	f := newGossipFixture(t)

	// node2 claims node1's slots (that's us) and node3's: both refused.
	f.b.handle(f.from("PING", "node2", []SlotRange{r(0, 10), r(100, 199), r(300, 310)}), f.inbox)
	f.wantOwner(5, "node1")
	f.wantOwner(305, "node3")
	f.wantOwner(150, "node2")

	// node3 lets go of its slots. node2's unchanged claim is tried again on
	// its next message and now succeeds; node1's slots still never move.
	f.b.handle(f.from("PING", "node3", nil), f.inbox)
	f.b.handle(f.from("PING", "node2", []SlotRange{r(0, 10), r(100, 199), r(300, 310)}), f.inbox)
	f.wantOwner(305, "node2")
	f.wantOwner(5, "node1")
}

func TestStrangerPingIsAnsweredButIgnored(t *testing.T) {
	f := newGossipFixture(t)
	before := len(f.l.Nodes)
	stranger := Message{Type: "PING", ID: "stranger", Name: "x", Addr: "127.0.0.1:7999",
		Slots:  []SlotRange{r(1000, 2000)},
		Gossip: []GossipEntry{{ID: "node9", Name: "node9", Addr: "127.0.0.1:7009"}}}
	f.b.handle(stranger, f.inbox)

	if len(f.inbox.out) != 1 {
		t.Fatalf("stranger got %d replies, want 1 PONG", len(f.inbox.out))
	}
	f.wantOwner(1500, "")
	if len(f.l.Nodes) != before {
		t.Fatal("a stranger's PING added nodes")
	}
}

func TestGossipAddsUnknownNodes(t *testing.T) {
	f := newGossipFixture(t)
	node2 := f.node("node2")
	before := len(f.l.Nodes)

	f.b.handle(f.from("PING", "node2", node2.Slots,
		GossipEntry{ID: "node4-id", Name: "node4", Addr: "127.0.0.1:7004"},                       // new: added
		GossipEntry{ID: f.node("node3").ID, Name: "node3", Addr: "127.0.0.1:7003"},               // known
		GossipEntry{ID: f.l.Self.ID, Name: "node1", Addr: "127.0.0.1:7001"},                      // us
		GossipEntry{ID: "node5-id", Name: "node5", Addr: "127.0.0.1:7005", Flags: FlagHandshake}, // unverified
		GossipEntry{ID: "node6-id", Name: "node6", Addr: "no-port"},                              // bad address
		GossipEntry{ID: "node7-id", Name: "node7", Addr: "127.0.0.1:60000"},                      // bus port too high
		GossipEntry{ID: "node8-id", Name: "node8", Addr: "127.0.0.1:7003"},                       // node3's address
	), f.inbox)

	if got := len(f.l.Nodes) - before; got != 1 {
		t.Fatalf("added %d nodes, want 1 (node4)", got)
	}
	p := f.b.Peer("node4-id")
	if p == nil {
		t.Fatal("node4 was not added")
	}
	if !p.Handshake || p.Connected() || p.Node.Port != 7004 || len(p.Node.Slots) != 0 {
		t.Fatalf("node4 = %+v, %+v; want handshake, no link, port 7004, no slots", p, p.Node)
	}
}

// A handshake peer is ignored until it answers on its own link; after
// that, its slots and gossip count.
func TestHandshakePeerIsTrustedAfterPongOnItsLink(t *testing.T) {
	f := newGossipFixture(t)
	f.b.handle(f.from("PING", "node2", nil,
		GossipEntry{ID: "node4-id", Name: "node4", Addr: "127.0.0.1:7004"}), f.inbox)
	p := f.b.Peer("node4-id")
	p.link = testLink() // as if tick had connected to it

	node4 := Message{Type: "PING", ID: "node4-id", Name: "node4", Addr: "127.0.0.1:7004",
		Slots:  []SlotRange{r(500, 599)},
		Gossip: []GossipEntry{{ID: "node9-id", Name: "node9", Addr: "127.0.0.1:7009"}}}

	// While in handshake, node4's slots and gossip are ignored.
	f.b.handle(node4, f.inbox)
	f.wantOwner(550, "")
	if f.b.Peer("node9-id") != nil {
		t.Fatal("gossip from a handshake peer was trusted")
	}

	// A PONG claiming to be node4, but on another connection, proves nothing.
	node4.Type = "PONG"
	f.b.handle(node4, f.inbox)
	if !p.Handshake {
		t.Fatal("a PONG on someone else's link ended the handshake")
	}

	// Its PONG on its own link ends the handshake, and from then on it's
	// listened to.
	f.b.handle(node4, p.link)
	if p.Handshake {
		t.Fatal("still in handshake after a PONG on its own link")
	}
	f.wantOwner(550, "node4")
	if f.b.Peer("node9-id") == nil {
		t.Fatal("gossip from node4 was ignored after the handshake")
	}
}

// ============================================================ sending gossip

func TestPickGossip(t *testing.T) {
	f := newGossipFixture(t)
	node2, node3 := f.node("node2"), f.node("node3")

	// With 3 nodes, node2 hears only about node3: not itself, not us.
	got := f.b.pickGossip(node2.ID)
	if len(got) != 1 || got[0].ID != node3.ID || got[0].Addr != node3.Addr {
		t.Fatalf("gossip to node2 = %+v, want just node3", got)
	}

	// A peer in handshake is never gossiped.
	f.b.Peer(node3.ID).Handshake = true
	if got := f.b.pickGossip(node2.ID); len(got) != 0 {
		t.Fatalf("gossiped a handshake peer: %+v", got)
	}

	// With 60 nodes, each message carries a tenth: 6, all different.
	for i := 4; i <= 60; i++ {
		f.b.addPeer(&Node{ID: fmt.Sprintf("id-%d", i), Name: fmt.Sprintf("node%d", i),
			Addr: fmt.Sprintf("127.0.0.1:%d", 7000+i), Port: 7000 + i}, false)
	}
	got = f.b.pickGossip(node2.ID)
	seen := map[string]bool{}
	for _, g := range got {
		if g.ID == node2.ID || g.ID == f.l.Self.ID || g.ID == node3.ID || seen[g.ID] {
			t.Fatalf("bad gossip entry %+v in %+v", g, got)
		}
		seen[g.ID] = true
	}
	if len(got) != 6 {
		t.Fatalf("got %d gossip entries, want 6", len(got))
	}
}

// What we send carries our slots and gossip for that receiver.
func TestMessagesCarryGossip(t *testing.T) {
	f := newGossipFixture(t)
	node2 := f.node("node2")
	f.b.handle(f.from("PING", "node2", node2.Slots), f.inbox)

	pong := mustDecodeOut(t, f.inbox)
	if pong.Type != "PONG" || !reflect.DeepEqual(pong.Slots, []SlotRange{r(0, 99)}) {
		t.Fatalf("reply = %+v, want a PONG with our slots 0-99", pong)
	}
	if len(pong.Gossip) != 1 || pong.Gossip[0].Name != "node3" {
		t.Fatalf("reply gossip = %+v, want node3", pong.Gossip)
	}
}

// mustDecodeOut takes the oldest message waiting in l's outbox and decodes it.
func mustDecodeOut(t *testing.T, l *link) Message {
	t.Helper()
	select {
	case raw := <-l.out:
		v, err := core.Decode(bufio.NewReader(bytes.NewReader(raw)))
		if err != nil {
			t.Fatal(err)
		}
		m, err := decodeMessage(v)
		if err != nil {
			t.Fatal(err)
		}
		return m
	default:
		t.Fatal("nothing was sent")
		return Message{}
	}
}

// ===================================================== MEET and handshakes

func TestMeetFromStranger(t *testing.T) {
	f := newGossipFixture(t)
	meet := Message{Type: "MEET", ID: "node4-id", Name: "node4", Addr: "127.0.0.1:7004",
		Slots: []SlotRange{r(500, 599)}}
	f.b.handle(meet, f.inbox)

	if len(f.inbox.out) != 1 {
		t.Fatalf("MEET got %d replies, want 1 PONG", len(f.inbox.out))
	}
	p := f.b.Peer("node4-id")
	if p == nil || !p.Handshake || p.Node.Port != 7004 {
		t.Fatalf("node4 = %+v; want it added, in handshake", p)
	}
	f.wantOwner(550, "") // not until it answers us on our own connection to it

	// A MEET from a node we know is just a PING: answered, nothing added.
	before := len(f.l.Nodes)
	f.b.handle(f.from("MEET", "node2", f.node("node2").Slots), f.inbox)
	if len(f.l.Nodes) != before || len(f.inbox.out) != 2 {
		t.Fatal("a MEET from a known node wasn't treated as a PING")
	}

	// A MEET claiming an address we already have is answered but not added.
	f.b.handle(Message{Type: "MEET", ID: "imposter", Name: "x", Addr: "127.0.0.1:7002"}, f.inbox)
	if f.b.Peer("imposter") != nil {
		t.Fatal("added a second node at node2's address")
	}
}

// The reader goroutine reports a closed link by the ID the peer had when
// we dialed. If the peer was renamed since, it must still lose its link.
func TestLinkDownFindsARenamedPeer(t *testing.T) {
	f := newGossipFixture(t)
	p := f.b.Peer(f.node("node2").ID)
	l := p.link
	f.b.linkDown("the-id-when-we-dialed", l)
	if p.Connected() {
		t.Fatal("peer kept a dead link")
	}
}

func TestHandshakeTimeout(t *testing.T) {
	f := newGossipFixture(t)
	f.b.handle(Message{Type: "MEET", ID: "node4-id", Name: "node4", Addr: "127.0.0.1:7004"}, f.inbox)
	f.b.handle(f.from("PING", "node2", f.node("node2").Slots,
		GossipEntry{ID: "node5-id", Name: "node5", Addr: "127.0.0.1:7005"}), f.inbox)
	conf := f.b.Peer(f.node("node3").ID)
	conf.Handshake = true // node3 from cluster.conf hasn't answered either

	old := time.Now().Add(-handshakeTimeout - time.Second)
	for _, p := range f.b.peers {
		p.created = old
	}
	f.b.tick()

	if f.b.Peer("node4-id") != nil || f.b.Peer("node5-id") != nil {
		t.Fatal("handshake peers that never answered weren't forgotten")
	}
	if f.b.Peer(conf.Node.ID) == nil {
		t.Fatal("a node from cluster.conf was forgotten")
	}
	if len(f.l.Nodes) != 3 {
		t.Fatalf("layout has %d nodes, want the 3 from cluster.conf", len(f.l.Nodes))
	}
}

// ============================================================= CLUSTER MEET

func TestClusterMeetRenamesOnPong(t *testing.T) {
	f := newGossipFixture(t)
	before := len(f.l.Nodes)
	if err := f.b.Meet("127.0.0.1", 7004); err != nil {
		t.Fatal(err)
	}
	tmp := f.b.peerByAddr("127.0.0.1:7004")
	if tmp == nil || !tmp.Handshake || len(tmp.Node.ID) != 40 {
		t.Fatalf("Meet added %+v; want a handshake peer with a 40-char temporary ID", tmp)
	}
	tmpID, l := tmp.Node.ID, testLink()
	tmp.link = l // as if tick had connected to it

	// Its PONG tells us its real ID and slots.
	f.b.handle(Message{Type: "PONG", ID: "node4-id", Name: "node4", Addr: "127.0.0.1:7004",
		Slots: []SlotRange{r(500, 599)}}, l)

	p := f.b.Peer("node4-id")
	if p != tmp || f.b.Peer(tmpID) != nil || p.Handshake || p.Node.Name != "node4" {
		t.Fatalf("after PONG: peer %+v (temporary still there: %v)", p, f.b.Peer(tmpID) != nil)
	}
	f.wantOwner(550, "node4")
	if len(f.l.Nodes) != before+1 {
		t.Fatalf("layout has %d nodes, want %d", len(f.l.Nodes), before+1)
	}

	// The reader goroutine still knows the link by the temporary ID.
	f.b.linkDown(tmpID, l)
	if p.Connected() {
		t.Fatal("renamed peer kept a dead link")
	}
}

func TestMeetingAKnownNodeDropsTheDuplicate(t *testing.T) {
	f := newGossipFixture(t)
	before := len(f.l.Nodes)
	f.b.Meet("127.0.0.2", 7002) // node2, under a second address
	tmp := f.b.peerByAddr("127.0.0.2:7002")
	tmp.link = testLink()

	f.b.handle(f.from("PONG", "node2", f.node("node2").Slots), tmp.link)
	if f.b.peerByAddr("127.0.0.2:7002") != nil || len(f.l.Nodes) != before {
		t.Fatal("the duplicate of node2 wasn't dropped")
	}
	select {
	case <-tmp.link.closed:
	default:
		t.Fatal("the duplicate's link wasn't closed")
	}
	f.wantOwner(150, "node2")
}

func TestMeetChecksTheAddress(t *testing.T) {
	f := newGossipFixture(t)
	before := len(f.l.Nodes)
	for _, bad := range []struct {
		host string
		port int
	}{{"localhost", 7004}, {"127.0.0.1", 0}, {"127.0.0.1", 60000}, {"", 7004}} {
		if err := f.b.Meet(bad.host, bad.port); err == nil {
			t.Errorf("Meet(%q, %d) succeeded", bad.host, bad.port)
		}
	}
	// Ourselves, and a node we know: fine, but nothing to do.
	if f.b.Meet("127.0.0.1", 7001) != nil || f.b.Meet("127.0.0.1", 7002) != nil {
		t.Fatal("Meet of a known address failed")
	}
	if len(f.l.Nodes) != before {
		t.Fatalf("layout has %d nodes, want %d", len(f.l.Nodes), before)
	}
}

// peerByAddr finds a peer by its client address (for peers whose ID we
// don't control, like the temporary one Meet makes).
func (b *Bus) peerByAddr(addr string) *Peer {
	for _, p := range b.peers {
		if p.Node.Addr == addr {
			return p
		}
	}
	return nil
}
