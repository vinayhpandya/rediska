package store

import (
	"testing"
	"time"
)

// fakeClock gives a store a clock the test controls.
func fakeClock(s *Store) *time.Time {
	now := time.Unix(1_000_000, 0)
	s.now = func() time.Time { return now }
	return &now
}

func TestAppendOnHyperLogLog(t *testing.T) {
	freshStats(t)
	s := New()
	s.PFAdd("h", "a")
	if _, err := s.Append("h", "x"); err != ErrWrongType {
		t.Fatalf("APPEND on a HyperLogLog: err = %v, want ErrWrongType (it used to crash)", err)
	}
	if n, _ := s.PFCount("h"); n != 1 {
		t.Errorf("HyperLogLog changed: PFCOUNT = %d, want 1", n)
	}
}

func TestAppendToExpiredKey(t *testing.T) {
	freshStats(t)
	s := New()
	now := fakeClock(s)
	s.Set("k", "old", time.Second)
	*now = now.Add(2 * time.Second)
	n, err := s.Append("k", "new")
	if err != nil || n != 3 {
		t.Fatalf("APPEND to an expired key: %d, %v; want 3, nil", n, err)
	}
	if v, _ := s.Get("k"); v != "new" {
		t.Errorf("value = %q, want %q (the expired value must not be kept)", v, "new")
	}
	if s.TTL("k") != -1 {
		t.Errorf("TTL = %d, want -1: the old expiry must not carry over", s.TTL("k"))
	}
}

func TestPFMergeRefusesOtherTypes(t *testing.T) {
	freshStats(t)
	s := New()
	s.Set("name", "vinay", 0)
	s.PFAdd("h", "a")
	if err := s.PFMerge("name", "h"); err != ErrNotHLL {
		t.Fatalf("PFMERGE into a string: err = %v, want ErrNotHLL", err)
	}
	if v, _ := s.Get("name"); v != "vinay" {
		t.Fatalf("PFMERGE destroyed the string: GET name = %q", v)
	}
	if err := s.PFMerge("dest", "h", "name"); err != ErrNotHLL {
		t.Fatalf("PFMERGE from a string: err = %v, want ErrNotHLL", err)
	}
	if _, exists := s.data["dest"]; exists {
		t.Error("a refused PFMERGE must not create dest")
	}
}

func TestPFMergeKeepsExpiry(t *testing.T) {
	freshStats(t)
	s := New()
	fakeClock(s)
	s.PFAdd("d", "a")
	e := s.data["d"]
	e.expiresAt = s.now().Add(100 * time.Second) // like EXPIRE d 100
	s.data["d"] = e
	s.PFAdd("src", "b")
	if err := s.PFMerge("d", "src"); err != nil {
		t.Fatal(err)
	}
	if ttl := s.TTL("d"); ttl != 100 {
		t.Errorf("TTL after PFMERGE = %d, want 100 (Redis keeps it)", ttl)
	}
	if n, _ := s.PFCount("d"); n != 2 {
		t.Errorf("PFCOUNT d = %d, want 2", n)
	}
}

func TestPFCountSeveralKeys(t *testing.T) {
	freshStats(t)
	s := New()
	s.PFAdd("h1", "a", "b")
	s.PFAdd("h2", "b", "c")
	if n, err := s.PFCount("h1", "h2", "missing"); err != nil || n != 3 {
		t.Fatalf("PFCOUNT h1 h2 missing = %d, %v; want 3 (a, b, c)", n, err)
	}
	if len(s.data) != 2 {
		t.Error("PFCOUNT of several keys must not store anything")
	}
	s.Set("name", "vinay", 0)
	if _, err := s.PFCount("name"); err != ErrNotHLL {
		t.Errorf("PFCOUNT on a string: err = %v, want ErrNotHLL", err)
	}
	if _, err := s.PFCount("h1", "name"); err != ErrNotHLL {
		t.Errorf("PFCOUNT h1 name: err = %v, want ErrNotHLL", err)
	}
}
