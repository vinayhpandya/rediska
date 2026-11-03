// Package replication copies a master's data to its replicas.
//
// A replica connects to its master's client port and sends SYNC. The
// master then sends it two things over that connection, in one ordered
// stream of commands:
//
//   - a snapshot: every key, as the command that recreates it (see
//     store.SlotCommands), copied one slot at a time, and
//   - every change made since, as the same entries the AOF records.
//
// The snapshot is copied a few slots at a time, between client commands,
// so clients never wait for the whole dataset to be copied. Copying and
// changes stay consistent through one rule: a change is sent to a replica
// only if its slot has already been copied to that replica. A change to a
// slot that hasn't been copied yet is skipped, because the slot will be
// copied later with the change already in it. This works because in
// cluster mode every command's keys are in one slot.
package replication

import (
	"bufio"
	"context"
	"io"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/vinayhpandya/rediska/cluster"
	"github.com/vinayhpandya/rediska/core"
	"github.com/vinayhpandya/rediska/store"
)

const (
	// queueSize is how many writes may wait to be sent to one replica, and
	// maxQueueBytes (below) how many bytes. A replica that falls this far
	// behind is disconnected, so it can't make the master hold an
	// unbounded backlog; it reconnects and syncs again.
	queueSize = 4096

	// While the snapshot is being copied, the next slots are copied only
	// once the replica's queue has drained below both of these, so the
	// snapshot moves at the replica's speed and never piles up in memory.
	lowWater      = 64
	lowWaterBytes = 4 << 20

	writeTimeout = 10 * time.Second
)

// maxQueueBytes is how many bytes may wait to be sent to one replica (see
// queueSize). Counting bytes, not just writes, matters when values are
// large: 4096 writes of 10 MB each would be 40 GB. A variable for tests.
var maxQueueBytes int64 = 64 << 20

// BatchKeys and BatchBytes bound one copying step, so that it never holds
// up clients for long: a step stops once it has copied about BatchKeys
// keys or BatchBytes bytes, whichever comes first. Bytes matter because
// one 10 MB value is one key, but far from a cheap one. A step always
// copies whole slots (see the package comment), so a slot bigger than
// this is still copied in one step. Variables so tests can make copying
// take many steps.
var (
	BatchKeys  = 1000
	BatchBytes = 1 << 20
)

// Entry is one change to send to replicas: a command, and the slot its
// keys are in.
type Entry struct {
	Slot int
	Args []string
}

// Master keeps track of the replicas connected to this node. Everything
// except a replica's queue belongs to the dispatch worker: the methods
// below must only be called there, except Serve.
type Master struct {
	replicas map[*replica]struct{}

	// OnChange, if set, is called on the dispatch worker whenever a
	// replica connects or goes away.
	OnChange func()
}

// Default is this node's Master.
var Default = NewMaster()

// NewMaster returns a Master with no replicas.
func NewMaster() *Master {
	return &Master{replicas: map[*replica]struct{}{}}
}

// replica is one connected replica, as the master sees it.
type replica struct {
	addr   string
	out    chan []byte  // encoded commands waiting to be written, in order
	queued atomic.Int64 // bytes in out: added by the worker, taken by the writer

	cursor int // slots below cursor have been copied to it; worker only

	closeOnce sync.Once
	closed    chan struct{} // closed when the replica is dropped
}

func newReplica(addr string) *replica {
	return &replica{addr: addr, out: make(chan []byte, queueSize), closed: make(chan struct{})}
}

func (r *replica) close() { r.closeOnce.Do(func() { close(r.closed) }) }

// Count returns how many replicas are connected. Worker only.
func (m *Master) Count() int { return len(m.replicas) }

func (m *Master) add(r *replica) {
	m.replicas[r] = struct{}{}
	log.Printf("replication: replica %s connected; copying data", r.addr)
	m.changed()
}

func (m *Master) remove(r *replica) {
	if _, ok := m.replicas[r]; !ok {
		return
	}
	delete(m.replicas, r)
	r.close()
	m.changed()
}

func (m *Master) changed() {
	if m.OnChange != nil {
		m.OnChange()
	}
}

// send queues b for r without ever waiting: the dispatch worker must not
// block on a slow network. If r's queue is full, by count or by bytes, r
// is disconnected.
func (m *Master) send(r *replica, b []byte) {
	if r.queued.Load()+int64(len(b)) > maxQueueBytes {
		m.tooFarBehind(r)
		return
	}
	select {
	case r.out <- b:
		r.queued.Add(int64(len(b)))
	default:
		m.tooFarBehind(r)
	}
}

func (m *Master) tooFarBehind(r *replica) {
	log.Printf("replication: replica %s is too far behind; disconnecting it", r.addr)
	m.remove(r)
}

// take is the writer's side of send: the next queued write, counted out.
func (r *replica) take(b []byte) []byte {
	r.queued.Add(-int64(len(b)))
	return b
}

// Feed sends one change to every replica that already has e's slot.
// Worker only.
func (m *Master) Feed(e Entry) {
	var b []byte
	for r := range m.replicas {
		if e.Slot < r.cursor {
			if b == nil {
				b = core.EncodeArray(e.Args)
			}
			m.send(r, b)
		}
	}
}

// FeedTransaction sends the changes of one MULTI/EXEC to every replica,
// wrapped in MULTI and EXEC so the replica applies them all at once. Each
// replica gets only the entries whose slots it already has; one that has
// none of them gets nothing. Worker only.
func (m *Master) FeedTransaction(entries []Entry) {
	for r := range m.replicas {
		var b []byte
		for _, e := range entries {
			if e.Slot < r.cursor {
				b = append(b, core.EncodeArray(e.Args)...)
			}
		}
		if b != nil {
			block := core.EncodeArray([]string{"MULTI"})
			block = append(block, b...)
			block = append(block, core.EncodeArray([]string{"EXEC"})...)
			m.send(r, block)
		}
	}
}

// copyNext copies the next slots to r: whole slots, until about BatchKeys
// keys or BatchBytes bytes have been copied, or every slot has. It reports
// whether the copy is finished. Worker only.
func (m *Master) copyNext(r *replica) (finished bool) {
	if _, ok := m.replicas[r]; !ok {
		return true // dropped meanwhile
	}
	var b []byte
	keys := 0
	for r.cursor < cluster.NumSlots && keys < BatchKeys && len(b) < BatchBytes {
		for _, cmd := range store.Default.SlotCommands(r.cursor) {
			b = append(b, core.EncodeArray(cmd)...)
			keys++
		}
		r.cursor++ // from now on, changes to this slot are sent to r
	}
	if b != nil {
		m.send(r, b)
	}
	if r.cursor == cluster.NumSlots {
		log.Printf("replication: replica %s has all the data; sending changes as they happen", r.addr)
		return true
	}
	return false
}

// Serve feeds the replica on conn, which has just sent SYNC, until it
// disconnects or ctx is cancelled. It runs on the connection's goroutine;
// run must run a function on the dispatch worker and wait for it to
// finish, returning false if the server is shutting down.
func (m *Master) Serve(ctx context.Context, conn net.Conn, run func(func()) bool) {
	r := newReplica(conn.RemoteAddr().String())
	defer conn.Close()
	if !run(func() { m.add(r) }) {
		return
	}
	defer run(func() {
		if _, ok := m.replicas[r]; ok {
			log.Printf("replication: replica %s disconnected", r.addr)
			m.remove(r)
		}
	})

	// A replica never sends anything after SYNC, so a read only ends when
	// it hangs up. Notice that, rather than wait for a write to fail.
	go func() {
		io.Copy(io.Discard, conn)
		r.close()
	}()

	// Answer SYNC right away, even if there's nothing to copy: the replica
	// waits for this +OK before it counts itself as connected.
	w := bufio.NewWriter(conn)
	if !write(conn, w, core.EncodeSimpleString("OK"), r) {
		return
	}
	copying := true
	for {
		// While copying, ask for the next slots whenever the queue runs low.
		if copying && len(r.out) < lowWater && r.queued.Load() < lowWaterBytes {
			var finished bool
			if !run(func() { finished = m.copyNext(r) }) {
				return
			}
			copying = !finished
		}

		var b []byte
		if copying {
			select {
			case b = <-r.out:
				r.take(b)
			case <-r.closed:
				return
			case <-ctx.Done():
				return
			default:
				continue // nothing queued yet: copy more
			}
		} else {
			select {
			case b = <-r.out:
				r.take(b)
			case <-r.closed:
				return
			case <-ctx.Done():
				return
			}
		}
		if !write(conn, w, b, r) {
			return
		}
	}
}

// write sends b, plus anything else already queued for r, and flushes.
func write(conn net.Conn, w *bufio.Writer, b []byte, r *replica) bool {
	conn.SetWriteDeadline(time.Now().Add(writeTimeout))
	if _, err := w.Write(b); err != nil {
		return false
	}
	for len(r.out) > 0 && w.Buffered() < 64<<10 {
		if _, err := w.Write(r.take(<-r.out)); err != nil {
			return false
		}
	}
	return w.Flush() == nil
}
