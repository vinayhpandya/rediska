package store

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"
	"time"

	"github.com/vinayhpandya/rediska/cluster"
)

// checkIndex compares the slot index with a full scan of every key. Any
// code path that creates or removes a key without updating the index makes
// this fail.
func checkIndex(t *testing.T, s *Store, step string) {
	t.Helper()
	want := map[int]map[string]bool{}
	for key := range s.data {
		slot := cluster.KeyHashSlot(key)
		if want[slot] == nil {
			want[slot] = map[string]bool{}
		}
		want[slot][key] = true
	}
	// Every slot that has keys must list exactly those keys...
	for slot, keys := range want {
		if got := s.CountKeysInSlot(slot); got != len(keys) {
			t.Fatalf("after %s: slot %d has %d keys in the index, %d in the store", step, slot, got, len(keys))
		}
		for _, k := range s.KeysInSlot(slot, len(keys)+1) {
			if !keys[k] {
				t.Fatalf("after %s: index lists %q in slot %d, but the store doesn't have it", step, k, slot)
			}
		}
	}
	// ...and no other slot may list anything.
	indexed := 0
	for _, keys := range s.slots {
		indexed += len(keys)
	}
	if indexed != len(s.data) {
		t.Fatalf("after %s: index holds %d keys, store holds %d", step, indexed, len(s.data))
	}
}

// TestSlotIndexStaysCorrect runs thousands of random operations, including
// expiry and eviction, and checks the index against the store after each.
func TestSlotIndexStaysCorrect(t *testing.T) {
	freshStats(t)
	now := time.Unix(1_000_000, 0)
	s := New()
	s.now = func() time.Time { return now }
	s.SetPolicy("allkeys-lru")
	s.EnableSlotIndex()

	rng := rand.New(rand.NewSource(1))
	// Strings and HyperLogLogs share key names on purpose, so commands also
	// hit keys of the wrong type (which must be refused without side effects).
	key := func() string {
		if rng.Intn(4) == 0 {
			return fmt.Sprintf("{tag%d}.k%d", rng.Intn(5), rng.Intn(20)) // shared slots
		}
		return fmt.Sprintf("k%d", rng.Intn(300))
	}
	for i := 0; i < 5000; i++ {
		var step string
		switch op := rng.Intn(10); op {
		case 0, 1:
			k := key()
			s.Set(k, "v", 0)
			step = "SET " + k
		case 2:
			k := key()
			s.Set(k, "v", time.Duration(1+rng.Intn(5))*time.Second)
			step = "SET EX " + k
		case 3:
			k := key()
			s.Append(k, "x")
			step = "APPEND " + k
		case 4:
			k := key()
			s.Delete(k)
			step = "DEL " + k
		case 5:
			k := key()
			s.PFAdd(k, "a")
			step = "PFADD " + k
		case 6:
			d, a, b := key(), key(), key()
			s.PFMerge(d, a, b)
			step = "PFMERGE " + d
		case 7:
			now = now.Add(time.Duration(rng.Intn(3)) * time.Second) // let keys expire
			s.Get(key())                                            // lazy expiry
			step = "time passes + GET"
		case 8:
			s.DeleteExpired()
			step = "active expiry"
		case 9:
			s.PerformEvictions(5, s.UsedMemory()*9/10) // evict about 10%
			step = "eviction"
		}
		checkIndex(t, s, step)
	}
	if got := KeyspaceStat[0]["keys"]; got != len(s.data) {
		t.Errorf("INFO keys = %d, store has %d", got, len(s.data))
	}
}

func TestEnableSlotIndexIndexesExistingKeys(t *testing.T) {
	freshStats(t)
	s := New()
	s.Set("{user1}.name", "vinay", 0)
	s.Set("{user1}.email", "v@x.com", 0)
	if s.CountKeysInSlot(8106) != 0 {
		t.Fatal("index should be off until enabled")
	}
	s.EnableSlotIndex()
	got := s.KeysInSlot(8106, 10)
	sort.Strings(got)
	if fmt.Sprint(got) != "[{user1}.email {user1}.name]" || s.CountKeysInSlot(8106) != 2 {
		t.Fatalf("slot 8106: %v (count %d)", got, s.CountKeysInSlot(8106))
	}
	if n := len(s.KeysInSlot(8106, 1)); n != 1 {
		t.Errorf("KeysInSlot with count 1 returned %d keys", n)
	}
}
