package cluster

import (
	cryptorand "crypto/rand"
	"encoding/hex"
	"fmt"
	"log"
	"math/rand/v2"
	"net"
	"slices"
	"strconv"
	"time"
)

// What a node does with the slots and gossip in messages it receives.
// Everything here runs on dispatchWorker (from handle), like the rest of
// the bus's state changes.
//
// Two rules decide whom we listen to:
//
//   - Only a node we know and trust (a peer that is not in handshake) can
//     change our view. A stranger's PING still gets a PONG, but what it
//     says about itself or others is ignored.
//   - A node only speaks for its own slots. Gossip entries tell us that a
//     node exists, never what it owns; we learn that from the node itself,
//     once we're connected to it.

// learnFrom applies what m's sender says: its own slots, and the nodes in
// its gossip section.
func (b *Bus) learnFrom(m Message) {
	p := b.peers[m.ID]
	if p == nil || p.Handshake {
		return // a stranger, or a node that hasn't answered us yet
	}
	b.applySlots(p, m.Slots)
	for _, g := range m.Gossip {
		b.learnNode(g, p)
	}
}

// applySlots records the slots p announced. It's called for every message,
// so it does nothing when the announcement hasn't changed, unless some of
// it was refused last time (the other owner may have let go since).
func (b *Bus) applySlots(p *Peer, slots []SlotRange) {
	if p.slotsHeard && !p.refused && slices.Equal(slots, p.announced) {
		return
	}
	changed := !p.slotsHeard || !slices.Equal(slots, p.announced)
	before := slices.Clone(p.Node.Slots)

	refused := b.layout.SetSlots(p.Node, slots)
	p.announced, p.slotsHeard, p.refused = slices.Clone(slots), true, len(refused) > 0

	if !slices.Equal(before, p.Node.Slots) {
		log.Printf("cluster bus: %s now owns slots %v", p.Node.Name, p.Node.Slots)
	}
	if changed && len(refused) > 0 { // log a conflict once, not every second
		log.Printf("cluster bus: %s claims slots %v, which another node owns; keeping the current owner",
			p.Node.Name, refused)
	}
}

// learnNode adds a node that from told us about, if it's new to us. It
// starts in handshake: tick connects to it, and it becomes a full peer
// when it answers.
func (b *Bus) learnNode(g GossipEntry, from *Peer) {
	self := b.layout.Self
	switch {
	case g.ID == "" || g.ID == self.ID:
		return
	case b.peers[g.ID] != nil:
		return // already known: the usual case once the cluster has settled
	case g.Flags&FlagHandshake != 0:
		return // from hasn't heard back from it either, so don't pass it on
	}
	_, portStr, err := net.SplitHostPort(g.Addr)
	port, perr := strconv.Atoi(portStr)
	if err != nil || perr != nil || port < 1 || BusPort(port) > 65535 {
		log.Printf("cluster bus: %s gossiped %q with a bad address %q; ignoring it", from.Node.Name, g.Name, g.Addr)
		return
	}
	if g.Addr == self.Addr || b.addrInUse(g.Addr) {
		return // a different ID at an address we know: a restarted node, sorted out later
	}

	n := &Node{Name: g.Name, Addr: g.Addr, Port: port, ID: g.ID}
	b.addPeer(n, true)
	log.Printf("cluster bus: %s told us about %s (%s); connecting", from.Node.Name, n.Name, n.Addr)
}

// addPeer adds n to the layout and the peer table. tick will connect to it.
func (b *Bus) addPeer(n *Node, handshake bool) *Peer {
	b.layout.AddNode(n)
	p := &Peer{Node: n, Handshake: handshake, created: time.Now()}
	b.peers[n.ID] = p
	return p
}

// dropPeer forgets p: its connection, its slots, and its place in the
// layout, so CLUSTER NODES stops showing it.
func (b *Bus) dropPeer(p *Peer) {
	if p.link != nil {
		p.link.close()
	}
	delete(b.peers, p.Node.ID)
	b.layout.RemoveNode(p.Node)
}

// ================================================================== sending

// pickGossip chooses which nodes to tell the node with ID to about: a few
// random peers, never the receiver itself (we're in the header) and never
// a peer still in handshake (we can't vouch for it).
//
// How many: a tenth of the nodes we know, but at least 3, as Redis does.
// Each message carries a different sample, so over a few rounds everyone
// hears about everyone without any one message listing the whole cluster.
func (b *Bus) pickGossip(to string) []GossipEntry {
	var candidates []*Peer
	for _, p := range b.peers {
		if p.Node.ID != to && !p.Handshake {
			candidates = append(candidates, p)
		}
	}
	want := min(max(3, (len(b.peers)+1)/10), len(candidates))
	rand.Shuffle(len(candidates), func(i, j int) {
		candidates[i], candidates[j] = candidates[j], candidates[i]
	})
	out := make([]GossipEntry, 0, want)
	for _, p := range candidates[:want] {
		out = append(out, GossipEntry{
			ID: p.Node.ID, Name: p.Node.Name, Addr: p.Node.Addr,
			PingSent: unixMilli(p.PingSent), PongRecv: unixMilli(p.PongRecv),
		})
	}
	return out
}

// unixMilli turns a time into Unix milliseconds, with 0 for "never".
func unixMilli(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixMilli()
}

// =================================================================== MEET

// Meet is CLUSTER MEET: start a handshake with the node at host:port (its
// client port). We don't know its ID yet, so the peer gets a random one
// until the node's PONG tells us the real one (see renamePeer). tick
// connects to it, and the first message is a MEET, so it adds us too.
//
// Meeting a node we already know, or ourselves, does nothing.
func (b *Bus) Meet(host string, port int) error {
	if net.ParseIP(host) == nil || port < 1 || BusPort(port) > 65535 {
		return fmt.Errorf("invalid node address %s:%d", host, port)
	}
	addr := net.JoinHostPort(host, strconv.Itoa(port))
	if addr == b.layout.Self.Addr || b.addrInUse(addr) {
		return nil
	}
	n := &Node{Name: addr, Addr: addr, Port: port, ID: randomID()}
	b.addPeer(n, true)
	log.Printf("cluster bus: meeting %s", addr)
	return nil
}

// randomID is a temporary node ID: 40 hex characters, like a real one.
func randomID() string {
	var buf [20]byte
	cryptorand.Read(buf[:])
	return hex.EncodeToString(buf[:])
}

// meetFrom handles a MEET from a node we don't know. We add it, in
// handshake: tick connects back to the address it gave and sends it a
// MEET, and only its PONG on that connection makes us trust it. Anyone can
// claim an address in a message; only the node really there can answer.
func (b *Bus) meetFrom(m Message) {
	_, portStr, err := net.SplitHostPort(m.Addr)
	port, perr := strconv.Atoi(portStr)
	switch {
	case m.ID == "" || m.ID == b.layout.Self.ID:
		return
	case err != nil || perr != nil || port < 1 || BusPort(port) > 65535:
		log.Printf("cluster bus: MEET from %q with a bad address %q; ignoring it", m.Name, m.Addr)
		return
	case m.Addr == b.layout.Self.Addr || b.addrInUse(m.Addr):
		return // we're already meeting whoever is there; its PONG will sort it out
	}
	b.addPeer(&Node{Name: m.Name, Addr: m.Addr, Port: port, ID: m.ID}, true)
	log.Printf("cluster bus: %s (%s) asked to meet us; connecting back", m.Name, m.Addr)
}

// renamePeer gives a peer we met by address its real ID and name, which
// its first PONG told us. It returns the peer to carry on with, or nil if
// the ID turns out to be us or a node we already know by another route
// (then the temporary peer is dropped: we'd only be talking to it twice).
func (b *Bus) renamePeer(p *Peer, id, name string) *Peer {
	if id == "" || id == b.layout.Self.ID || b.peers[id] != nil {
		log.Printf("cluster bus: %s is a node we already know; dropping the duplicate", p.Node.Addr)
		b.dropPeer(p)
		return nil
	}
	delete(b.peers, p.Node.ID)
	p.Node.ID, p.Node.Name = id, name
	b.peers[id] = p
	return p
}

func (b *Bus) addrInUse(addr string) bool {
	for _, p := range b.peers {
		if p.Node.Addr == addr {
			return true
		}
	}
	return false
}
