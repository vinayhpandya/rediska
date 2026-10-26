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
// nodes. Nodes send each other PING ("are you there?") and PONG ("yes").
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
	Node     *Node     // which node this is (from cluster.conf)
	PingSent time.Time // PING sent and not answered yet (zero if none)
	PongRecv time.Time // last PONG received (zero if never)

	link       *link     // our connection to it; nil when not connected
	connecting bool      // a dial is in progress, don't start another
	lastDial   time.Time // when we last tried to connect
	lastPing   time.Time // when we last sent it a PING
}

// Connected reports whether we currently have a connection to the peer.
func (p *Peer) Connected() bool { return p.link != nil }

// Bus is this node's cluster bus.
type Bus struct {
	layout   *Layout
	peers    map[string]*Peer // the other nodes, by node ID
	listener net.Listener

	ctx     context.Context
	send    func(Event)    // hands an event to dispatchWorker
	wg      sync.WaitGroup // counts every bus goroutine, for Wait
	dialReq chan *Node     // tick -> dialLoop: "please connect to this node"
}

// CurrentBus is the running bus (nil when cluster mode is off, or in
// tests). CLUSTER NODES reads it. Only dispatchWorker may use it.
var CurrentBus *Bus

// ================================================================== setup
// Called by server.Run, in this order: NewBus, Listen, Start, Wait.

// NewBus makes one Peer for every node in the layout except me.
// Nothing connects yet.
func NewBus(l *Layout) *Bus {
	b := &Bus{
		layout:  l,
		peers:   map[string]*Peer{},
		dialReq: make(chan *Node, 64),
	}
	for _, n := range l.Nodes {
		if n != l.Self {
			b.peers[n.ID] = &Peer{Node: n} // every other field starts at zero
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

// self builds a message from this node: b.self("PING") or b.self("PONG").
func (b *Bus) self(typ string) Message {
	s := b.layout.Self
	return Message{Type: typ, ID: s.ID, Name: s.Name, Addr: s.Addr}
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
		case n := <-b.dialReq:
			b.wg.Add(1)
			go b.dial(n.ID, busAddr(n))
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

// tick runs every 100 ms: ask to connect to any peer we have no connection
// to (at most once a second), and PING each connected peer once a second.
func (b *Bus) tick() {
	now := time.Now()
	for _, p := range b.peers {
		if p.link == nil {
			if !p.connecting && now.Sub(p.lastDial) >= retryInterval {
				select {
				case b.dialReq <- p.Node: // ask dialLoop to connect
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

// ping sends p a PING and notes the time.
func (b *Bus) ping(p *Peer, now time.Time) {
	if p.link.send(b.self("PING")) {
		p.lastPing = now
		if p.PingSent.IsZero() {
			p.PingSent = now // keep the oldest unanswered PING, for failure detection later
		}
	}
}

// handle reacts to one message that arrived on link l.
func (b *Bus) handle(m Message, l *link) {
	switch m.Type {
	case "PING":
		l.send(b.self("PONG")) // answer on the connection it came in on
	case "PONG":
		if p := b.peers[m.ID]; p != nil {
			p.PongRecv = time.Now()
			p.PingSent = time.Time{}
		}
	}
}

// linkUp: dial succeeded. Save the connection and PING straight away.
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
	b.ping(p, time.Now())
}

// dialFailed: let tick try again (lastDial makes it wait a second).
func (b *Bus) dialFailed(id string) {
	if p := b.peers[id]; p != nil {
		p.connecting = false
	}
}

// linkDown: a connection we dialed closed. Forget it; tick will reconnect.
// Only clear it if it's still the current link, so a late report from an
// old connection can't wipe out a newer one.
func (b *Bus) linkDown(id string, l *link) {
	if p := b.peers[id]; p != nil && p.link == l {
		log.Printf("cluster bus: lost connection to %s", p.Node.Name)
		p.link = nil
	}
}
