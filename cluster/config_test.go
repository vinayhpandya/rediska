package cluster

import (
	"slices"
	"strings"
	"testing"
)

const goodConf = `
# name   address          slots
node1  127.0.0.1:7001  0-5460
node2  127.0.0.1:7002  5461-10922
node3  127.0.0.1:7003  10923-16383
`

func TestParseLayout(t *testing.T) {
	l, err := ParseLayout(strings.NewReader(goodConf), 7002)
	if err != nil {
		t.Fatal(err)
	}
	if len(l.Nodes) != 3 {
		t.Fatalf("got %d nodes, want 3", len(l.Nodes))
	}
	if l.Self.Name != "node2" {
		t.Fatalf("self = %s, want node2", l.Self.Name)
	}
	want := map[int]string{0: "node1", 5460: "node1", 5461: "node2",
		5798: "node2", 10922: "node2", 10923: "node3", 16383: "node3"}
	for slot, name := range want {
		if got := l.Owner(slot); got == nil || got.Name != name {
			t.Errorf("Owner(%d) = %v, want %s", slot, got, name)
		}
	}
	if len(l.Self.ID) != 40 {
		t.Errorf("ID %q is not 40 chars", l.Self.ID)
	}
	// IDs must be the same no matter which node parses the file.
	l2, _ := ParseLayout(strings.NewReader(goodConf), 7001)
	if l2.Nodes[1].ID != l.Self.ID {
		t.Error("node2's ID changed between parses")
	}
}

func TestUnownedSlots(t *testing.T) {
	l, err := ParseLayout(strings.NewReader("node1 127.0.0.1:7001 0-100 200"), 7001)
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range []int{101, 199, 201, -1, 16384} {
		if l.Owner(s) != nil {
			t.Errorf("Owner(%d) should be nil", s)
		}
	}
	if l.Owner(200) == nil {
		t.Error("single slot 200 should be owned")
	}
}

func TestParseLayoutErrors(t *testing.T) {
	cases := map[string]struct {
		conf string
		port int
	}{
		"overlap":        {"node1 127.0.0.1:7001 0-100\nnode2 127.0.0.1:7002 100-200", 7001},
		"out of range":   {"node1 127.0.0.1:7001 0-16384", 7001},
		"backwards":      {"node1 127.0.0.1:7001 200-100", 7001},
		"not a number":   {"node1 127.0.0.1:7001 abc", 7001},
		"missing slots":  {"node1 127.0.0.1:7001", 7001},
		"bad address":    {"node1 localhost 0-100", 7001},
		"duplicate name": {"node1 127.0.0.1:7001 0-10\nnode1 127.0.0.1:7002 11-20", 7001},
		"self not found": {"node1 127.0.0.1:7001 0-100", 9999},
		"empty":          {"# nothing here\n", 7001},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := ParseLayout(strings.NewReader(c.conf), c.port); err == nil {
				t.Fatal("expected an error")
			}
		})
	}
}

// ============================================================ runtime changes

// smallLayout is node1 (0-99), node2 (100-199) and node3 (300-399), as
// seen by node1. Slots 200-299 and 400+ belong to nobody.
func smallLayout(t *testing.T) *Layout {
	t.Helper()
	l, err := ParseLayout(strings.NewReader(`
node1 127.0.0.1:7001 0-99
node2 127.0.0.1:7002 100-199
node3 127.0.0.1:7003 300-399
`), 7001)
	if err != nil {
		t.Fatal(err)
	}
	return l
}

func ownerName(l *Layout, slot int) string {
	if n := l.Owner(slot); n != nil {
		return n.Name
	}
	return ""
}

func TestSetSlots(t *testing.T) {
	l := smallLayout(t)
	node2 := l.Nodes[1]

	// node2 now announces 100-299 plus node1's 42 and node3's 300-310.
	refused := l.SetSlots(node2, []SlotRange{{250, 299}, {100, 249}, {42, 42}, {300, 310}})
	if want := []SlotRange{{42, 42}, {300, 310}}; !slices.Equal(refused, want) {
		t.Fatalf("refused %v, want %v (they belong to node1 and node3)", refused, want)
	}
	if want := []SlotRange{{100, 299}}; !slices.Equal(node2.Slots, want) {
		t.Fatalf("node2.Slots = %v, want %v, merged and sorted", node2.Slots, want)
	}
	for slot, want := range map[int]string{42: "node1", 150: "node2", 250: "node2", 305: "node3"} {
		if got := ownerName(l, slot); got != want {
			t.Errorf("slot %d: owner %q, want %q", slot, got, want)
		}
	}

	// Its next announcement replaces the last one: slots it drops are freed.
	l.SetSlots(node2, []SlotRange{{100, 149}})
	if got := ownerName(l, 200); got != "" {
		t.Fatalf("slot 200 still owned by %q after node2 dropped it", got)
	}
}

func TestRemoveNode(t *testing.T) {
	l := smallLayout(t)
	node2 := l.Nodes[1]
	l.RemoveNode(node2)
	if len(l.Nodes) != 2 || l.Nodes[0].Name != "node1" || l.Nodes[1].Name != "node3" {
		t.Fatalf("nodes after removing node2: %v", l.Nodes)
	}
	if got := ownerName(l, 150); got != "" {
		t.Fatalf("slot 150 still owned by %q", got)
	}
}

func TestForgetOthersSlots(t *testing.T) {
	l := smallLayout(t)
	l.ForgetOthersSlots()
	for slot, want := range map[int]string{50: "node1", 150: "", 350: ""} {
		if got := ownerName(l, slot); got != want {
			t.Errorf("slot %d: owner %q, want %q", slot, got, want)
		}
	}
	if len(l.Nodes[1].Slots) != 0 || len(l.Nodes[0].Slots) != 1 {
		t.Fatalf("slots after: node1 %v, node2 %v", l.Nodes[0].Slots, l.Nodes[1].Slots)
	}
}
