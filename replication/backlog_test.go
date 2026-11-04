package replication

import (
	"bytes"
	"strings"
	"testing"

	"github.com/vinayhpandya/rediska/core"
)

func TestBacklogKeepsTheNewestChanges(t *testing.T) {
	b := newBacklog(10)
	for i := int64(1); i <= 5; i++ {
		b.add(i, []byte("abcd")) // 4 bytes each: only the last 2 fit in 10
	}
	if len(b.items) != 2 || b.items[0].offset != 4 || b.bytes != 8 {
		t.Fatalf("kept offsets %d.., %d items, %d bytes; want 4 and 5, 8 bytes",
			b.items[0].offset, len(b.items), b.bytes)
	}

	// One change bigger than the whole backlog is still kept, alone.
	b.add(6, []byte(strings.Repeat("x", 50)))
	if len(b.items) != 1 || b.items[0].offset != 6 {
		t.Fatalf("after a 50-byte change: %d items from offset %d, want just 6", len(b.items), b.items[0].offset)
	}
}

func TestBacklogSince(t *testing.T) {
	b := newBacklog(1 << 20)
	for i := int64(1); i <= 5; i++ {
		b.add(i, []byte{byte('0' + i)})
	}
	for _, c := range []struct {
		offset int64
		want   string
		ok     bool
	}{
		{5, "", true},      // up to date
		{3, "45", true},    // missed 4 and 5
		{0, "12345", true}, // missed everything, but it's all still here
		{6, "", false},     // ahead of the master
	} {
		got, ok := b.since(c.offset, 5)
		if ok != c.ok || string(got) != c.want {
			t.Errorf("since(%d) = %q, %v; want %q, %v", c.offset, got, ok, c.want, c.ok)
		}
	}

	b.add(6, bytes.Repeat([]byte("x"), 1<<20)) // pushes out 1-5
	if _, ok := b.since(3, 6); ok {
		t.Error("since(3) succeeded after changes 4 and 5 were dropped")
	}
	if got, ok := b.since(5, 6); !ok || len(got) != 1<<20 {
		t.Errorf("since(5): %d bytes, %v; want change 6", len(got), ok)
	}
}

// Changes are numbered only once replication has started: before the
// first replica connects, nothing is kept.
func TestNothingRecordedBeforeTheFirstReplica(t *testing.T) {
	testStore(t)
	m := NewMaster()
	m.Feed(Entry{Slot: 0, Args: []string{"SET", "k", "v"}})
	if m.Active() || m.Offset() != 0 {
		t.Fatalf("Active=%v, Offset=%d before any replica; want false, 0", m.Active(), m.Offset())
	}
	connected(m)
	m.Feed(Entry{Slot: 0, Args: []string{"SET", "k", "v"}})
	if m.Offset() != 1 {
		t.Fatalf("Offset = %d after one change, want 1", m.Offset())
	}
}

// The end of a full copy says which change the replica now has.
func TestCopyEndsWithTheOffset(t *testing.T) {
	testStore(t)
	m := NewMaster()
	r := connected(m)
	for range 3 {
		m.Feed(Entry{Slot: 0, Args: []string{"INCR", "n"}}) // offsets 1-3
	}
	queued(t, r) // nothing copied yet, so none of those were sent
	for !m.copyNext(r) {
	}
	got := queued(t, r)
	last := got[len(got)-1]
	if last[0] != "_SYNCED" || last[1] != m.ReplID() || last[2] != "3" {
		t.Fatalf("the copy ended with %q, want _SYNCED %s 3", last, m.ReplID())
	}
}

// Whether a replica can carry on, or needs a full copy.
func TestContinueOrFullCopy(t *testing.T) {
	testStore(t)
	m := NewMaster()
	m.BacklogSize = 200
	connected(m) // starts the backlog
	for range 5 {
		m.Feed(Entry{Slot: 0, Args: []string{"SET", "k", "v"}}) // offsets 1-5, 26 bytes each
	}
	setK := string(core.EncodeArray([]string{"SET", "k", "v"}))

	for _, c := range []struct {
		name   string
		replID string
		offset int64
		missed string // "" with full=false: up to date
		full   bool
	}{
		{"up to date", m.ReplID(), 5, "", false},
		{"missed two changes", m.ReplID(), 3, setK + setK, false},
		{"a new replica", "", -1, "", true},
		{"another master's history", "0123456789", 5, "", true},
		{"ahead of the master", m.ReplID(), 9, "", true},
	} {
		missed, why := m.missedSince(c.replID, c.offset)
		if (why != "") != c.full || string(missed) != c.missed {
			t.Errorf("%s: missed %q, why %q; want %q, full copy %v", c.name, missed, why, c.missed, c.full)
		}
	}

	// 10 more changes push offsets 1-5 out of the 200-byte backlog.
	for range 10 {
		m.Feed(Entry{Slot: 0, Args: []string{"SET", "k", "v"}})
	}
	if _, why := m.missedSince(m.ReplID(), 3); why == "" {
		t.Error("offset 3 can still continue after its changes left the backlog")
	}
}

// A transaction is one change, whoever receives it.
func TestTransactionIsOneChange(t *testing.T) {
	testStore(t)
	m := NewMaster()
	r := connected(m)
	r.cursor = 1000 // still copying: slot 5000 isn't there yet
	m.FeedTransaction([]Entry{
		{Slot: 0, Args: []string{"SET", "a", "1"}},
		{Slot: 5000, Args: []string{"SET", "b", "1"}},
	})
	if m.Offset() != 1 {
		t.Fatalf("Offset = %d after one transaction, want 1", m.Offset())
	}
	// The backlog keeps all of it, for replicas that resume later...
	full, _ := m.backlog.since(0, 1)
	if !bytes.Contains(full, []byte("$1\r\nb\r\n")) {
		t.Fatal("the backlog's copy of the transaction is missing the change to slot 5000")
	}
	// ...while this replica, still copying, got only the slot it has.
	if got := queued(t, r); len(got) != 3 || got[1][1] != "a" {
		t.Fatalf("sent %q, want MULTI, SET a, EXEC", got)
	}
}
