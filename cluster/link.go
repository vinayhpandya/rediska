package cluster

import (
	"net"
	"sync"
	"time"
)

// link is one bus connection. dispatchWorker never writes to the socket
// itself: it puts messages in out, and a writer goroutine writes them.
type link struct {
	out       chan []byte   // the outbox: encoded messages waiting to be written
	closed    chan struct{} // closed when the link is closed; stops the writer
	closeOnce sync.Once     // makes close() safe to call more than once
	conn      net.Conn      // the socket; close() closes it (nil in unit tests)
}

// startLink makes the outbox for conn and starts its writer goroutine.
// (sendQueueSize = 64 and writeTimeout = 1s are constants in bus.go.)
func (b *Bus) startLink(conn net.Conn) *link {
	l := &link{
		out:    make(chan []byte, sendQueueSize), // room for 64 messages
		closed: make(chan struct{}),
		conn:   conn,
	}
	b.wg.Add(1) // count the writer, so Wait waits for it
	go func() { // ---- the writer goroutine ----
		defer b.wg.Done()
		for {
			select {
			case msg := <-l.out:
				// A message is waiting: write it. If the peer is stuck,
				// give up after writeTimeout instead of waiting forever.
				conn.SetWriteDeadline(time.Now().Add(writeTimeout))
				if _, err := conn.Write(msg); err != nil {
					conn.Close() // readLoop sees this and reports linkDown
					return
				}
			case <-l.closed:
				return // the link was closed (and with it, conn)
			case <-b.ctx.Done():
				conn.Close() // server is shutting down
				return
			}
		}
	}()
	return l
}

// send puts m in the outbox and returns at once. It never waits: if the
// outbox is full (the peer is slow or gone), m is dropped and send
// returns false. The next PING, a second later, will try again.
func (l *link) send(m Message) bool {
	select {
	case l.out <- m.encode():
		return true
	default:
		return false
	}
}

// close stops the writer and closes the socket, which makes readLoop
// return too. Safe to call more than once, from any goroutine.
//
// Closing the socket matters when dispatchWorker drops a link it doesn't
// want (an unknown peer in linkUp, or a peer it forgets): before, only the
// writer stopped, and the reader sat on the open socket until the other
// side hung up.
func (l *link) close() {
	l.closeOnce.Do(func() {
		close(l.closed)
		if l.conn != nil {
			l.conn.Close()
		}
	})
}
