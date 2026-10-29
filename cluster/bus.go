package cluster

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/vinayhpandya/rediska/core"
)

// The cluster bus is a second TCP port on every node, used only by other
// nodes. Nodes send each other PING ("are you there?") and PONG ("yes"),
// and MEET ("are you there? and add me if you don't know me"). Every
// message also carries the sender's slots and a little gossip about other
// nodes; gossip.go is what nodes do with that.
//
// Threading: goroutines that wait on the network or timers never touch the
// bus's state. Anything that reads or changes peers runs on dispatchWorker,
// handed over as an Event (see events.go). That keeps Rediska's "one goroutine owns the
// state, no locks" design.

const (
	// BusPortOffset: a node on port 7001 listens for other nodes on 17001.
	BusPortOffset = 10000

	cronInterval  = 100 * time.Millisecond // how often tick runs
	pingInterval  = time.Second            // how often each peer gets a PING
	retryInterval = time.Second            // wait before reconnecting
	dialTimeout   = time.Second
	writeTimeout  = time.Second
	sendQueueSize = 64 // messages waiting to be written, per connection
)

// handshakeTimeout: a node we heard of (by CLUSTER MEET, a MEET or gossip)
// is forgotten if it hasn't answered us in this long. Nodes listed in
// cluster.conf are never forgotten. A variable so tests can shorten it.
var handshakeTimeout = 10 * time.Second

// NodeTimeout is how long another node can stay silent before we suspect
// it has failed (PFAIL). main sets it from --cluster-node-timeout; the
// default is Redis's. NewBus copies it into Bus.NodeTimeout.
var NodeTimeout = 15 * time.Second

// BusPort is the bus port for a client port: 7002 -> 17002.
func BusPort(port int) int { return port + BusPortOffset }

// busAddr is where to dial a node's bus: same host, port + 10000.
// Node{Addr: "127.0.0.1:7002", Port: 7002} -> "127.0.0.1:17002"
func busAddr(n *Node) string {
	host, _, _ := net.SplitHostPort(n.Addr)
	return net.JoinHostPort(host, strconv.Itoa(BusPort(n.Port)))
}

// Peer is the live status of one other node. Only dispatchWorker touches it.
type Peer struct {
	Node     *Node     // which node this is (from cluster.conf, or learned by gossip)
	PingSent time.Time // PING sent and not answered yet (zero if none)
	PongRecv time.Time // last PONG received (zero if never)

	// Handshake is true for a node we heard about but that hasn't answered
	// us yet. We don't trust what it says (its slots or gossip) until it does.
	Handshake bool

	link       *link     // our connection to it; nil when not connected
	connecting bool      // a dial is in progress, don't start another
	lastDial   time.Time // when we last tried to connect
	lastPing   time.Time // when we last sent it a PING

	announced  []SlotRange // the slots it last announced, to skip repeats
	slotsHeard bool        // announced is set (it may announce none at all)
	refused    bool        // some of announced belonged to others; retry each time

	created  time.Time // when we added it, for handshakeTimeout
	fromConf bool      // listed in cluster.conf: never forgotten

	flags       NodeFlags // FlagPFail or FlagFail: what we think of its health
	failReports map[string]time.Time
}

// Connected reports whether we currently have a connection to the peer.
func (p *Peer) Connected() bool { return p.link != nil }

// PFail reports whether we suspect the peer has failed: it hasn't answered
// us for longer than the node timeout. Only our opinion.
func (p *Peer) PFail() bool { return p.flags&FlagPFail != 0 }

// Failed reports whether the peer is marked FAIL: a majority of masters
// agreed it has failed.
func (p *Peer) Failed() bool { return p.flags&FlagFail != 0 }

// Bus is this node's cluster bus.
type Bus struct {
	layout      *Layout
	peers       map[string]*Peer // the other nodes, by node ID
	listener    net.Listener
	NodeTimeout time.Duration

	ctx     context.Context
	send    func(Event)     // hands an event to dispatchWorker
	wg      sync.WaitGroup  // counts every bus goroutine, for Wait
	dialReq chan dialTarget // tick -> dialLoop: "please connect to this node"
}

// dialTarget is what dialLoop needs to connect to a peer. tick copies it
// out of the peer, because the peer belongs to dispatchWorker: dialLoop
// mustn't read it (renamePeer changes its ID, for one).
type dialTarget struct {
	id   string // peer's ID when the dial started, for the events it causes
	addr string // its bus address
}

// CurrentBus is the running bus (nil when cluster mode is off, or in
// tests). CLUSTER NODES reads it. Only dispatchWorker may use it.
var CurrentBus *Bus

// ================================================================== setup
// Called by server.Run, in this order: NewBus, Listen, Start, Wait.

// NewBus makes one Peer for every node in the layout except me. They start
// in handshake: we trust a node's slots once it has answered us. Nothing
// connects yet.
func NewBus(l *Layout) *Bus {
	b := &Bus{
		layout:      l,
		peers:       map[string]*Peer{},
		NodeTimeout: NodeTimeout,
		dialReq:     make(chan dialTarget, 64),
	}
	now := time.Now()
	for _, n := range l.Nodes {
		if n != l.Self {
			b.peers[n.ID] = &Peer{Node: n, Handshake: true, fromConf: true, created: now}
		}
	}
	return b
}

// Listen opens the bus port (client port + 10000). It's separate from
// Start so the server can stop with a clear error, before starting
// anything else, if the port is taken.
func (b *Bus) Listen(host string) error {
	addr := net.JoinHostPort(host, strconv.Itoa(BusPort(b.layout.Self.Port)))
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("cluster bus: listen on %s: %w", addr, err)
	}
	b.listener = ln
	return nil
}

// Start starts the bus's goroutines; they run until ctx is cancelled.
// send must hand an event to dispatchWorker (which calls Handle), and must
// not block forever once ctx is cancelled.
//
// Each goroutine waits for a different thing:
//   - acceptLoop: another node connecting to us
//   - cronLoop:   the 100 ms timer
//   - dialLoop:   tick asking us to connect to a node
//   - the last one: shutdown, to close the listener (which wakes acceptLoop)
func (b *Bus) Start(ctx context.Context, send func(Event)) {
	b.ctx, b.send = ctx, send
	b.wg.Add(4)
	go b.acceptLoop()
	go b.cronLoop()
	go b.dialLoop()
	go func() {
		defer b.wg.Done()
		<-ctx.Done()
		b.listener.Close() // Accept returns an error, so acceptLoop stops
	}()
}

// Wait blocks until every bus goroutine has stopped, after ctx is
// cancelled. Connections close themselves (see startLink in link.go).
func (b *Bus) Wait() { b.wg.Wait() }

// Peers returns this node's peers, in layout order. dispatchWorker only.
func (b *Bus) Peers() []*Peer {
	var ps []*Peer
	for _, n := range b.layout.Nodes {
		if p := b.peers[n.ID]; p != nil {
			ps = append(ps, p)
		}
	}
	return ps
}

// Peer returns the peer with this node ID, or nil. dispatchWorker only.
func (b *Bus) Peer(id string) *Peer { return b.peers[id] }

// msg builds a message from this node to the node with ID to: our header
// (with our own slots), and gossip picked for that receiver (gossip.go).
func (b *Bus) msg(typ, to string) Message {
	s := b.layout.Self
	return Message{Type: typ, ID: s.ID, Name: s.Name, Addr: s.Addr, Slots: s.Slots,
		Gossip: b.pickGossip(to)}
}

// ======================================================= network goroutines
// These only wait for things and send them to dispatchWorker with b.send.
// They never touch peers. New goroutines are only started from goroutines
// already counted in b.wg, so Wait can't miss one at shutdown.

// acceptLoop waits for other nodes to connect to our bus port, and starts a
// writer and a reader for each connection.
func (b *Bus) acceptLoop() {
	defer b.wg.Done()
	for {
		conn, err := b.listener.Accept()
		if err != nil {
			if b.ctx.Err() != nil {
				return // shutting down
			}
			continue
		}
		l := b.startLink(conn)
		b.wg.Add(1)
		go b.readLoop(conn, l, "") // incoming: we only answer on it
	}
}

// cronLoop sends dispatchWorker a tick event every 100 ms.
func (b *Bus) cronLoop() {
	defer b.wg.Done()
	t := time.NewTicker(cronInterval)
	defer t.Stop()
	for {
		select {
		case <-b.ctx.Done():
			return
		case <-t.C:
			b.send(Event{Kind: EvTick})
		}
	}
}

// dialLoop waits for tick to ask for a connection, and starts a dial for
// each request. (dispatchWorker doesn't start goroutines itself: it isn't
// counted in b.wg, so at shutdown that could race with Wait.)
func (b *Bus) dialLoop() {
	defer b.wg.Done()
	for {
		select {
		case <-b.ctx.Done():
			return
		case t := <-b.dialReq:
			b.wg.Add(1)
			go b.dial(t.id, t.addr)
		}
	}
}

// dial connects to a peer's bus port. On success it starts the
// connection's writer and reader, then tells dispatchWorker (linkUp).
func (b *Bus) dial(id, addr string) {
	defer b.wg.Done()
	d := net.Dialer{Timeout: dialTimeout}
	conn, err := d.DialContext(b.ctx, "tcp", addr)
	if err != nil {
		b.send(Event{Kind: EvDialFailed, ID: id})
		return
	}
	l := b.startLink(conn)
	b.wg.Add(1)
	go b.readLoop(conn, l, id)
	b.send(Event{Kind: EvLinkUp, ID: id, Link: l})
}

// readLoop reads messages from one connection and hands each to
// dispatchWorker (handle). When the connection closes, it tells
// dispatchWorker (linkDown). peerID is the peer for connections we dialed,
// "" for incoming ones.
func (b *Bus) readLoop(conn net.Conn, l *link, peerID string) {
	defer b.wg.Done()
	r := bufio.NewReader(conn)
	for {
		v, err := core.Decode(r)
		if err != nil {
			break // closed, or garbage on the wire
		}
		m, err := decodeMessage(v)
		if err != nil {
			log.Printf("%v (from %s)", err, conn.RemoteAddr())
			break
		}
		b.send(Event{Kind: EvMessage, Msg: m, Link: l})
	}
	conn.Close()
	l.close()
	if peerID != "" {
		b.send(Event{Kind: EvLinkDown, ID: peerID, Link: l})
	}
}

// ========================================================= dispatchWorker
// Everything below is called from Handle (events.go), on dispatchWorker,
// one at a time, so these functions can read and change peers freely.
// They must never block.

// tick runs every 100 ms: forget handshake peers that never answered, ask
// to connect to any peer we have no connection to (at most once a second),
// and PING each connected peer once a second.
func (b *Bus) tick() {
	now := time.Now()
	for _, p := range b.peers {
		if p.Handshake && !p.fromConf && now.Sub(p.created) >= handshakeTimeout {
			log.Printf("cluster bus: %s (%s) never answered; forgetting it", p.Node.Name, p.Node.Addr)
			b.dropPeer(p) // deleting from a map while ranging over it is fine in Go
			continue
		}
		b.checkTimeout(p, now)
		if p.link == nil {
			if !p.connecting && now.Sub(p.lastDial) >= retryInterval {
				select {
				case b.dialReq <- dialTarget{p.Node.ID, busAddr(p.Node)}: // ask dialLoop to connect
					p.connecting, p.lastDial = true, now
				default: // dialLoop is busy; try on a later tick
				}
			}
			continue
		}
		if now.Sub(p.lastPing) >= pingInterval {
			b.ping(p, now)
		}
	}
}

// ping sends p a PING and notes the time. A peer that hasn't answered yet
// gets a MEET instead, so it adds us if it doesn't know us.
func (b *Bus) ping(p *Peer, now time.Time) {
	typ := "PING"
	if p.Handshake {
		typ = "MEET"
	}
	b.sendTo(p, typ, now)
}

// sendTo sends p a message of type typ on our link to it, noting the time
// as a PING sent.
func (b *Bus) sendTo(p *Peer, typ string, now time.Time) {
	if p.link.send(b.msg(typ, p.Node.ID)) {
		p.lastPing = now
		if p.PingSent.IsZero() {
			p.PingSent = now // keep the oldest unanswered PING, for failure detection later
		}
	}
}

// handle reacts to one message that arrived on link l.
func (b *Bus) handle(m Message, l *link) {
	switch m.Type {
	case "MEET":
		if b.peers[m.ID] == nil {
			b.meetFrom(m) // a node we don't know: add it, and check it (gossip.go)
		}
		l.send(b.msg("PONG", m.ID)) // otherwise, a MEET is just a PING
	case "PING":
		l.send(b.msg("PONG", m.ID)) // answer on the connection it came in on
	case "PONG":
		b.pong(m, l)
	case "FAIL":
		b.failFrom(m)
	}
	b.learnFrom(m) // its slots and gossip (gossip.go)
}

// pong handles a PONG. PONGs answer our PINGs and MEETs, so they arrive on
// links we dialed. Only a PONG on a peer's own link counts as that peer
// answering: anyone can put any ID in a message, but only the node at that
// address can answer on a connection we opened to it.
func (b *Bus) pong(m Message, l *link) {
	p := b.peerOn(l) // the peer we dialed on this connection
	if p == nil {
		return // not on a link of ours: proves nothing
	}
	if p.Node.ID != m.ID {
		if !p.Handshake {
			return // a trusted peer now answering with another ID: not handled yet
		}
		// A peer we only knew by address (CLUSTER MEET), now telling us its
		// real ID. If that ID is one we already know, this was a duplicate.
		if p = b.renamePeer(p, m.ID, m.Name); p == nil {
			return
		}
	}
	p.PongRecv = time.Now()
	p.PingSent = time.Time{}
	b.clearFailure(p) // it answered, so it's alive (failure.go)
	if p.Handshake {
		p.Handshake = false // it answered: it's real
		log.Printf("cluster bus: handshake with %s (%s) done", p.Node.Name, p.Node.Addr)
	}
}

// peerOn returns the peer whose current link is l, or nil.
func (b *Bus) peerOn(l *link) *Peer {
	for _, p := range b.peers {
		if p.link == l {
			return p
		}
	}
	return nil
}

// linkUp: dial succeeded. Save the connection and say hello straight away.
//
// The first message on every new connection is a MEET, even to a peer we
// know: if it restarted, it has forgotten us (nodes don't save what they
// learned yet), and a PING alone wouldn't make it add us back. To a node
// that knows us, a MEET is just a PING.
func (b *Bus) linkUp(id string, l *link) {
	p := b.peers[id]
	if p == nil || b.ctx.Err() != nil {
		l.close()
		return
	}
	p.connecting = false
	if p.link == nil {
		log.Printf("cluster bus: connected to %s (%s)", p.Node.Name, busAddr(p.Node))
	}
	p.link = l
	b.sendTo(p, "MEET", time.Now())
}

// dialFailed: let tick try again (lastDial makes it wait a second).
func (b *Bus) dialFailed(id string) {
	if p := b.peers[id]; p != nil {
		p.connecting = false
		startWaiting(p, time.Now())
	}
}

// linkDown: a connection we dialed closed. Forget it; tick will reconnect.
// Only clear it if it's still the current link, so a late report from an
// old connection can't wipe out a newer one.
//
// id is the peer's ID when we dialed. If the peer was renamed since (see
// renamePeer), we find it by its link instead.
func (b *Bus) linkDown(id string, l *link) {
	p := b.peers[id]
	if p == nil || p.link != l {
		p = b.peerOn(l)
	}
	if p != nil {
		log.Printf("cluster bus: lost connection to %s", p.Node.Name)
		p.link = nil
		startWaiting(p, time.Now())
	}
}
