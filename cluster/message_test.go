package cluster

import (
	"bufio"
	"bytes"
	"reflect"
	"strings"
	"testing"

	"github.com/vinayhpandya/rediska/core"
)

// roundTrip encodes m, reads it back the way readLoop does (core.Decode,
// then decodeMessage), and returns the result.
func roundTrip(t *testing.T, m Message) Message {
	t.Helper()
	v, err := core.Decode(bufio.NewReader(bytes.NewReader(m.encode())))
	if err != nil {
		t.Fatalf("core.Decode: %v", err)
	}
	got, err := decodeMessage(v)
	if err != nil {
		t.Fatalf("decodeMessage: %v", err)
	}
	return got
}

func TestMessageRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		m    Message
	}{
		{"empty slots and gossip", Message{
			Type: "PING", ID: "abc", Name: "node1", Addr: "127.0.0.1:7001",
		}},
		{"MEET", Message{
			Type: "MEET", ID: "abc", Name: "node1", Addr: "127.0.0.1:7001",
			Slots: []SlotRange{{0, 5460}},
		}},
		{"everything set", Message{
			Type: "PONG", ID: "abc", Name: "node1", Addr: "127.0.0.1:7001", Flags: 3,
			Slots: []SlotRange{{0, 0}, {5, 5460}, {16383, 16383}},
			Gossip: []GossipEntry{
				{ID: "def", Name: "node2", Addr: "127.0.0.1:7002", Flags: 1, PingSent: 1761512345678, PongRecv: 0},
				{ID: "ghi", Name: "node3", Addr: "10.0.0.3:7003", PingSent: 0, PongRecv: 1761512345999},
			},
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := roundTrip(t, tt.m)
			if !reflect.DeepEqual(got, tt.m) {
				t.Fatalf("round trip changed the message\n got: %+v\nwant: %+v", got, tt.m)
			}
		})
	}
}

// The hand-typed PING in message.go's comment and docs/cluster.md must
// stay valid.
func TestHandTypedPing(t *testing.T) {
	raw := "*7\r\n$4\r\nPING\r\n$1\r\nx\r\n$4\r\ntest\r\n$14\r\n127.0.0.1:9999\r\n:0\r\n*0\r\n*0\r\n"
	v, err := core.Decode(bufio.NewReader(strings.NewReader(raw)))
	if err != nil {
		t.Fatal(err)
	}
	m, err := decodeMessage(v)
	if err != nil {
		t.Fatal(err)
	}
	if m.Type != "PING" || m.ID != "x" || m.Name != "test" || m.Addr != "127.0.0.1:9999" {
		t.Fatalf("got %+v", m)
	}
}

func TestDecodeMessageRejects(t *testing.T) {
	// good builds a valid message as decoded RESP, for each case to break.
	good := func() []any {
		return []any{"PING", "id", "name", "127.0.0.1:7001", int64(0),
			[]any{int64(0), int64(100)},
			[]any{[]any{"id2", "n2", "127.0.0.1:7002", int64(0), int64(0), int64(0)}},
		}
	}
	tests := []struct {
		name   string
		mutate func(m []any) any
	}{
		{"not an array", func(m []any) any { return "PING" }},
		{"old 4-item format", func(m []any) any { return []any{"PING", "id", "name", "addr"} }},
		{"too many items", func(m []any) any { return append(m, "extra") }},
		{"unknown type", func(m []any) any { m[0] = "HELLO"; return m }},
		{"id not a string", func(m []any) any { m[1] = int64(7); return m }},
		{"flags not an integer", func(m []any) any { m[4] = "0"; return m }},
		{"flags too big", func(m []any) any { m[4] = int64(1 << 16); return m }},
		{"negative flags", func(m []any) any { m[4] = int64(-1); return m }},
		{"slots not an array", func(m []any) any { m[5] = "0-100"; return m }},
		{"odd number of slot ints", func(m []any) any { m[5] = []any{int64(0)}; return m }},
		{"slot not an integer", func(m []any) any { m[5] = []any{"0", int64(1)}; return m }},
		{"slot above 16383", func(m []any) any { m[5] = []any{int64(0), int64(16384)}; return m }},
		{"negative slot", func(m []any) any { m[5] = []any{int64(-1), int64(5)}; return m }},
		{"slot start after end", func(m []any) any { m[5] = []any{int64(10), int64(5)}; return m }},
		{"gossip not an array", func(m []any) any { m[6] = "x"; return m }},
		{"gossip entry not an array", func(m []any) any { m[6] = []any{"x"}; return m }},
		{"gossip entry too short", func(m []any) any {
			m[6] = []any{[]any{"id2", "n2", "addr", int64(0), int64(0)}}
			return m
		}},
		{"gossip addr not a string", func(m []any) any {
			m[6] = []any{[]any{"id2", "n2", int64(1), int64(0), int64(0), int64(0)}}
			return m
		}},
		{"gossip negative pong time", func(m []any) any {
			m[6] = []any{[]any{"id2", "n2", "addr", int64(0), int64(0), int64(-5)}}
			return m
		}},
	}

	if _, err := decodeMessage(good()); err != nil {
		t.Fatalf("the valid base message was rejected: %v", err)
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if m, err := decodeMessage(tt.mutate(good())); err == nil {
				t.Fatalf("accepted a bad message: %+v", m)
			}
		})
	}
}

// What a node sends must carry its own slots, from the layout.
func TestSelfMessageCarriesOwnSlots(t *testing.T) {
	me := &Node{Name: "node1", Addr: "127.0.0.1:7001", Port: 7001, ID: "me",
		Slots: []SlotRange{{0, 99}, {200, 299}}}
	other := &Node{Name: "node2", Addr: "127.0.0.1:7002", Port: 7002, ID: "other",
		Slots: []SlotRange{{100, 199}}}
	b := NewBus(&Layout{Nodes: []*Node{me, other}, Self: me})

	got := roundTrip(t, b.msg("PING", other.ID))
	if !reflect.DeepEqual(got.Slots, me.Slots) {
		t.Fatalf("slots = %v, want %v", got.Slots, me.Slots)
	}
	if len(got.Gossip) != 0 { // the only other node is the receiver itself
		t.Fatalf("gossip = %v, want none", got.Gossip)
	}
}
