// Package replication copies a master's data to its replicas.
//
// A replica connects to its master's client port and sends PSYNC. If it
// was connected before and the master still has every change it missed in
// its backlog (backlog.go), the master sends just those changes, then new
// ones as they happen. Otherwise the master sends it two things, in one
// ordered stream of commands:
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
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
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

	// replID names this master's history of changes, and offset is the
	// offset of the newest change it recorded. A replica that comes back
	// with the same replID and an offset still in the backlog can carry on
	// from there. replID is new every time the process starts, because the
	// backlog doesn't survive a restart.
	replID  string
	offset  int64
	backlog *backlog // nil until the first replica connects

	// BacklogSize is how many bytes of recent changes the backlog keeps
	// (--repl-backlog-size). Set it before any replica connects.
	BacklogSize int

	fullSyncs, partialSyncs int // for INFO replication

	// OnChange, if set, is called on the dispatch worker whenever a
	// replica connects or goes away.
	OnChange func()
}

// DefaultBacklogSize is the backlog size if --repl-backlog-size isn't
// given: 1 MB, as in Redis.
const DefaultBacklogSize = 1 << 20

// Default is this node's Master.
var Default = NewMaster()

// NewMaster returns a Master with no replicas and a new replication ID.
func NewMaster() *Master {
	id := make([]byte, 20)
	rand.Read(id)
	return &Master{
		replicas:    map[*replica]struct{}{},
		replID:      hex.EncodeToString(id),
		BacklogSize: DefaultBacklogSize,
	}
}

// ReplID, Offset, FullSyncs and PartialSyncs are for INFO replication.
// Worker only.
func (m *Master) ReplID() string    { return m.replID }
func (m *Master) Offset() int64     { return m.offset }
func (m *Master) FullSyncs() int    { return m.fullSyncs }
func (m *Master) PartialSyncs() int { return m.partialSyncs }

// Active reports whether changes need recording for replicas: from the
// moment the first replica connects, every change goes into the backlog,
// even while no replica is connected, because that's exactly when one is
// missing changes. Worker only.
func (m *Master) Active() bool { return m.backlog != nil }

// record gives a change the next offset and keeps it in the backlog.
func (m *Master) record(data []byte) {
	m.offset++
	m.backlog.add(m.offset, data)
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
	if m.backlog == nil {
		m.backlog = newBacklog(m.BacklogSize)
	}
	m.replicas[r] = struct{}{}
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

// Feed records one change in the backlog, and sends it to every replica
// that already has e's slot. Worker only.
func (m *Master) Feed(e Entry) {
	if !m.Active() {
		return
	}
	b := core.EncodeArray(e.Args)
	m.record(b)
	for r := range m.replicas {
		if e.Slot < r.cursor {
			m.send(r, b)
		}
	}
}

// FeedTransaction records the changes of one MULTI/EXEC in the backlog as
// one change, and sends them to every replica, wrapped in MULTI and EXEC
// so the replica applies them all at once. A replica still being copied
// gets only the entries whose slots it already has; one that has none of
// them gets nothing. Worker only.
func (m *Master) FeedTransaction(entries []Entry) {
	if !m.Active() {
		return
	}
	full := transaction(entries, func(Entry) bool { return true })
	m.record(full)
	for r := range m.replicas {
		if r.cursor == cluster.NumSlots {
			m.send(r, full)
		} else if b := transaction(entries, func(e Entry) bool { return e.Slot < r.cursor }); b != nil {
			m.send(r, b)
		}
	}
}

// transaction encodes the entries that keep accepts as one MULTI/EXEC
// block, or returns nil if it accepts none.
func transaction(entries []Entry, keep func(Entry) bool) []byte {
	var b []byte
	for _, e := range entries {
		if keep(e) {
			b = append(b, core.EncodeArray(e.Args)...)
		}
	}
	if b == nil {
		return nil
	}
	block := core.EncodeArray([]string{"MULTI"})
	block = append(block, b...)
	return append(block, core.EncodeArray([]string{"EXEC"})...)
}

// copyNext copies the next slots to r: whole slots, until about BatchKeys
// keys or BatchBytes bytes have been copied, or every slot has. It reports
// whether the copy is finished.
//
// When it is, it also sends _SYNCED <replID> <offset>: from here on the
// replica has everything up to offset, and counts each change it applies,
// so that if it disconnects it can ask for just what it missed. Before the
// copy is finished, its data doesn't match any one offset. Worker only.
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
	finished = r.cursor == cluster.NumSlots
	if finished {
		b = append(b, core.EncodeArray([]string{"_SYNCED", m.replID, strconv.FormatInt(m.offset, 10)})...)
	}
	if b != nil {
		m.send(r, b)
	}
	if finished {
		log.Printf("replication: replica %s has all the data; sending changes as they happen", r.addr)
	}
	return finished
}

// missedSince returns the changes a replica that has everything up to
// offset in history replID is missing. why is "" if the backlog has them
// all, and otherwise says why the replica needs a full copy. Worker only.
func (m *Master) missedSince(replID string, offset int64) (missed []byte, why string) {
	switch {
	case replID == "":
		return nil, "it has no data yet"
	case replID != m.replID:
		return nil, "its data came from another master, or from before this one restarted"
	case offset > m.offset:
		return nil, fmt.Sprintf("it claims change %d, but this master is only at %d", offset, m.offset)
	}
	missed, ok := m.backlog.since(offset, m.offset)
	if !ok {
		return nil, fmt.Sprintf("it's at change %d, and the backlog no longer goes back that far", offset)
	}
	return missed, ""
}

// ParsePSync reads PSYNC's arguments: the replication ID and offset the
// replica already has, or "?" and -1 if it has nothing. It returns "" for
// "?".
func ParsePSync(args []string) (replID string, offset int64, err error) {
	if len(args) != 2 {
		return "", 0, fmt.Errorf("ERR wrong number of arguments for 'psync' command")
	}
	offset, err = strconv.ParseInt(args[1], 10, 64)
	if err != nil {
		return "", 0, fmt.Errorf("ERR value is not an integer or out of range")
	}
	if args[0] != "?" {
		replID = args[0]
	}
	return replID, offset, nil
}

// Serve feeds the replica on conn, which has just sent PSYNC replID
// offset (see ParsePSync), until it disconnects or ctx is cancelled. It
// runs on the connection's goroutine; run must run a function on the
// dispatch worker and wait for it to finish, returning false if the server
// is shutting down.
//
// If the backlog has every change the replica missed, Serve answers
// +CONTINUE and sends just those; otherwise it answers +FULLRESYNC and
// copies everything.
func (m *Master) Serve(ctx context.Context, conn net.Conn, run func(func()) bool, replID string, offset int64) {
	r := newReplica(conn.RemoteAddr().String())
	defer conn.Close()

	// Deciding, registering the replica and queueing what it missed happen
	// in one worker step, so no change can fall between "what you missed"
	// and "what's new".
	full := true
	if !run(func() {
		m.add(r)
		missed, why := m.missedSince(replID, offset)
		if why != "" {
			m.fullSyncs++
			log.Printf("replication: full resync for replica %s: %s", r.addr, why)
			return
		}
		full = false
		m.partialSyncs++
		r.cursor = cluster.NumSlots // it has every slot already
		log.Printf("replication: partial resync for replica %s: sending the %d changes it missed",
			r.addr, m.offset-offset)
		if missed != nil {
			m.send(r, missed)
		}
	}) {
		return
	}
	defer run(func() {
		if _, ok := m.replicas[r]; ok {
			log.Printf("replication: replica %s disconnected", r.addr)
			m.remove(r)
		}
	})

	// A replica never sends anything after PSYNC, so a read only ends when
	// it hangs up. Notice that, rather than wait for a write to fail.
	go func() {
		io.Copy(io.Discard, conn)
		r.close()
	}()

	// Answer right away, even if there's nothing to send: the replica waits
	// for this before it counts itself as connected.
	answer := "CONTINUE"
	if full {
		answer = "FULLRESYNC"
	}
	w := bufio.NewWriter(conn)
	if !write(conn, w, core.EncodeSimpleString(answer), r) {
		return
	}
	copying := full
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
