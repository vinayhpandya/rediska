package hll

import (
	"fmt"
	"testing"
)

func TestBytesRoundTrip(t *testing.T) {
	for _, n := range []int{0, 10, 20000} { // empty, sparse, and dense
		h := New()
		for i := range n {
			h.Add([]byte(fmt.Sprint(i)))
		}
		got, err := FromBytes(h.Bytes())
		if err != nil {
			t.Fatalf("%d items: %v", n, err)
		}
		if got.Encoding() != h.Encoding() || got.Count() != h.Count() {
			t.Fatalf("%d items: got %s/%d, want %s/%d",
				n, got.Encoding(), got.Count(), h.Encoding(), h.Count())
		}
	}
}

func TestFromBytesRejectsGarbage(t *testing.T) {
	for _, b := range [][]byte{nil, {0}, {1, 2, 3}, {7, 0, 0}} {
		if _, err := FromBytes(b); err == nil {
			t.Errorf("FromBytes(%v) accepted bad input", b)
		}
	}
}
