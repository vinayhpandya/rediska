package replication

import (
	"bufio"
	"bytes"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/vinayhpandya/rediska/cluster"
	"github.com/vinayhpandya/rediska/core"
	"github.com/vinayhpandya/rediska/store"
)

// These tests call the Master's worker-only methods directly, as the
// dispatch worker would, and read what was queued for a replica.

// testStore gives the test its own store.Default, with the slot index on.
func testStore(t *testing.T) *store.Store {
	t.Helper()
	old := store.Default
	s := store.New()
	s.EnableSlotIndex()
	store.Default = s
	t.Cleanup(func() { store.Default = old })
	return s
}

// queued decodes every command waiting in r's queue.
func queued(t *testing.T, r *replica) [][]string {
	t.Helper()
	var buf bytes.Buffer
	for len(r.out) > 0 {
		buf.Write(r.take(<-r.out)) // as the writer does: counts the bytes out
	}
	var out [][]string
	br := bufio.NewReader(&buf)
	for br.Buffered() > 0 || buf.Len() > 0 {
		v, err := core.Decode(br)
		if err != nil {
			break
		}
		args, err := toArgs(v)
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, args)
	}
	return out
}

func connected(m *Master) *replica {
	r := newReplica("test")
	m.add(r)
	return r
}

// Keys in two different slots: a is in a lower slot than b.
const keyA, keyB = "age", "visits" // slots 741 and 12263

func TestChangesFollowTheCursor(t *testing.T) {
	testStore(t)
	m := NewMaster()
	r := connected(m)
	slotA, slotB := cluster.KeyHashSlot(keyA), cluster.KeyHashSlot(keyB)

	r.cursor = slotA + 1 // slotA has been copied; slotB hasn't
	m.Feed(Entry{Slot: slotA, Args: []string{"SET", keyA, "1"}})
	m.Feed(Entry{Slot: slotB, Args: []string{"SET", keyB, "1"}})

	want := [][]string{{"SET", keyA, "1"}} // slotB will be copied later, with its new value
	if got := queued(t, r); !reflect.DeepEqual(got, want) {
		t.Fatalf("sent %q, want %q", got, want)
	}
}

func TestTransactionKeepsOnlyCopiedSlots(t *testing.T) {
	testStore(t)
	m := NewMaster()
	r := connected(m)
	slotA, slotB := cluster.KeyHashSlot(keyA), cluster.KeyHashSlot(keyB)

	tx := []Entry{
		{Slot: slotA, Args: []string{"SET", keyA, "1"}},
		{Slot: slotB, Args: []string{"DEL", keyB}}, // e.g. an eviction during the transaction
		{Slot: slotA, Args: []string{"INCR", keyA}},
	}
	m.FeedTransaction(tx) // cursor 0: nothing copied, so nothing to send
	if got := queued(t, r); got != nil {
		t.Fatalf("sent %q before any slot was copied", got)
	}

	r.cursor = slotA + 1
	m.FeedTransaction(tx)
	want := [][]string{{"MULTI"}, {"SET", keyA, "1"}, {"INCR", keyA}, {"EXEC"}}
	if got := queued(t, r); !reflect.DeepEqual(got, want) {
		t.Fatalf("sent %q, want %q", got, want)
	}
}

// Each copying step copies whole slots, about BatchKeys keys at a time,
// and changes to a slot are sent only once it has been copied.
func TestCopyGoesSlotBySlot(t *testing.T) {
	s := testStore(t)
	old := BatchKeys
	BatchKeys = 3
	t.Cleanup(func() { BatchKeys = old })

	// 5 keys in slot 741 (hash tag {age}), 1 key in slot 12263.
	for i := range 5 {
		s.Set(fmt.Sprintf("{age}.%d", i), "x", 0)
	}
	s.Set(keyB, "1", 0)

	m := NewMaster()
	r := connected(m)
	if m.copyNext(r) {
		t.Fatal("finished after one step")
	}
	// The whole of slot 741 went in one step, though it's more than 3 keys.
	if got := len(queued(t, r)); got != 5 {
		t.Fatalf("first step copied %d keys, want all 5 of slot 741", got)
	}
	if r.cursor != cluster.KeyHashSlot("{age}")+1 {
		t.Fatalf("cursor = %d, want just past slot 741", r.cursor)
	}

	// A change to slot 741 is sent now; one to 12263 isn't yet.
	m.Feed(Entry{Slot: 741, Args: []string{"SET", "{age}.0", "y"}})
	m.Feed(Entry{Slot: 12263, Args: []string{"SET", keyB, "2"}})
	if got := queued(t, r); len(got) != 1 || got[0][1] != "{age}.0" {
		t.Fatalf("sent %q, want only the change to slot 741", got)
	}

	if !m.copyNext(r) {
		t.Fatal("not finished after copying every slot")
	}
	// visits, then the marker saying the copy is finished.
	if got := queued(t, r); len(got) != 2 || got[0][1] != keyB || got[1][0] != "_SYNCED" {
		t.Fatalf("second step sent %q, want visits and _SYNCED", got)
	}
}

// The worker never waits for a slow replica: one whose queue is full is
// disconnected.
func TestSlowReplicaIsDisconnected(t *testing.T) {
	testStore(t)
	m := NewMaster()
	changes := 0
	m.OnChange = func() { changes++ }
	r := connected(m)
	r.cursor = cluster.NumSlots

	for range queueSize {
		m.Feed(Entry{Slot: 0, Args: []string{"SET", "k", "v"}})
	}
	if m.Count() != 1 {
		t.Fatal("dropped before the queue was full")
	}
	m.Feed(Entry{Slot: 0, Args: []string{"SET", "k", "v"}}) // one too many
	if m.Count() != 0 {
		t.Fatal("a replica with a full queue is still connected")
	}
	select {
	case <-r.closed:
	default:
		t.Fatal("the dropped replica wasn't closed")
	}
	if changes != 2 { // connected, then dropped
		t.Fatalf("OnChange called %d times, want 2", changes)
	}
}

// A copying step also stops at about BatchBytes, so a few large values
// don't make one long step.
func TestCopyStepStopsAtBatchBytes(t *testing.T) {
	s := testStore(t)
	oldBytes := BatchBytes
	BatchBytes = 1 << 20
	t.Cleanup(func() { BatchBytes = oldBytes })

	big := strings.Repeat("x", 600<<10)       // 600 KB each, in 3 different slots
	keys := []string{"age", "name", "visits"} // slots 741, 5798, 12263
	for _, k := range keys {
		s.Set(k, big, 0)
	}

	m := NewMaster()
	r := connected(m)
	m.copyNext(r)
	// After 2 values (1.2 MB) the step is over the limit, so it stops.
	got := queued(t, r)
	if len(got) != 2 || got[0][1] != "age" || got[1][1] != "name" {
		t.Fatalf("first step copied %d keys, want age and name", len(got))
	}
	if r.queued.Load() != 0 {
		t.Fatalf("%d bytes still counted as queued after reading them all", r.queued.Load())
	}
	if !m.copyNext(r) || len(queued(t, r)) != 2 { // visits, and _SYNCED
		t.Fatal("the second step should copy visits and finish")
	}
}

// A replica is disconnected once the bytes waiting for it pass the limit,
// even if few writes are waiting.
func TestReplicaOverByteLimitIsDisconnected(t *testing.T) {
	testStore(t)
	old := maxQueueBytes
	maxQueueBytes = 1000
	t.Cleanup(func() { maxQueueBytes = old })

	m := NewMaster()
	r := connected(m)
	r.cursor = cluster.NumSlots
	value := strings.Repeat("v", 300)
	for range 3 { // about 3 x 320 bytes: under the limit
		m.Feed(Entry{Slot: 0, Args: []string{"SET", "k", value}})
	}
	if m.Count() != 1 {
		t.Fatalf("dropped at %d queued bytes, under the 1000-byte limit", r.queued.Load())
	}
	m.Feed(Entry{Slot: 0, Args: []string{"SET", "k", value}}) // over it
	if m.Count() != 0 {
		t.Fatalf("still connected with %d bytes queued", r.queued.Load())
	}
}
