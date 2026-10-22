package cluster

import (
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
