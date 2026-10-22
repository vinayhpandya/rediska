package store

import "testing"

// freshStats resets the global key counters for one test.
func freshStats(t *testing.T) {
	t.Helper()
	old := KeyspaceStat[0]
	KeyspaceStat[0] = map[string]int{}
	t.Cleanup(func() { KeyspaceStat[0] = old })
}

func TestPFAddCountsKeyAndMemory(t *testing.T) {
	freshStats(t)
	s := New()
	if _, err := s.PFAdd("visits", "a", "b"); err != nil {
		t.Fatal(err)
	}
	if got := KeyspaceStat[0]["keys"]; got != 1 {
		t.Errorf("keys = %d after PFADD, want 1", got)
	}
	afterOne := s.UsedMemory()
	if afterOne <= int64(len("visits")) {
		t.Errorf("UsedMemory = %d: the HyperLogLog's own bytes aren't counted", afterOne)
	}
	// Adding many values grows the sketch; memory must follow.
	for i := 0; i < 2000; i++ {
		s.PFAdd("visits", string(rune('a'+i%26))+string(rune(i)))
	}
	if s.UsedMemory() <= afterOne {
		t.Errorf("UsedMemory didn't grow with the sketch: %d -> %d", afterOne, s.UsedMemory())
	}
	// Deleting it must bring both counters back to zero.
	s.Delete("visits")
	if got := KeyspaceStat[0]["keys"]; got != 0 {
		t.Errorf("keys = %d after DEL, want 0 (it went negative before the fix)", got)
	}
	if s.UsedMemory() != 0 {
		t.Errorf("UsedMemory = %d after DEL, want 0", s.UsedMemory())
	}
}

func TestPFAddWrongType(t *testing.T) {
	freshStats(t)
	s := New()
	s.Set("name", "vinay", 0)
	changed, err := s.PFAdd("name", "a")
	if err != ErrNotHLL || changed {
		t.Fatalf("PFAdd on a string: changed=%v err=%v, want false, ErrNotHLL", changed, err)
	}
	if v, _ := s.Get("name"); v != "vinay" {
		t.Errorf("string value was replaced: got %q", v)
	}
}

func TestPFAddCreatesEmptyKey(t *testing.T) {
	freshStats(t)
	s := New()
	if changed, _ := s.PFAdd("h1"); !changed {
		t.Error("PFADD h1 (new key, no values) should report a change, like Redis's 1")
	}
	if changed, _ := s.PFAdd("h1"); changed {
		t.Error("PFADD h1 again should report no change")
	}
	if changed, _ := s.PFAdd("h1", "a"); !changed {
		t.Error("adding a new value should report a change")
	}
}
