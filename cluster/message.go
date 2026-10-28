package cluster

import (
	"fmt"

	"github.com/vinayhpandya/rediska/core"
)

// A bus message is a RESP array of 7 items:
//
//	1  type      bulk string   PING, PONG or MEET
//	2  id        bulk string   sender's node ID
//	3  name      bulk string   sender's name
//	4  addr      bulk string   sender's client address, "host:port"
//	5  flags     integer       sender's flags (see NodeFlags)
//	6  slots     array         sender's OWN slots, as integers:
//	                           start1 end1 start2 end2 ...
//	7  gossip    array         a few other nodes the sender knows, each an
//	                           array of 6: id name addr flags ping-sent pong-recv
//
// MEET is a PING that also says "add me if you don't know me": it's the
// only message that makes a node take on a stranger (see gossip.go).
//
// Items 1-6 are the header: what the sender says about itself. A node only
// ever announces its own slots; gossip entries carry who exists, not who
// owns what. Ping-sent and pong-recv are the sender's view of that node, as
// Unix milliseconds (0 means none pending / never).
//
// A PING with no slots and no gossip, typed by hand:
//
//	*7\r\n$4\r\nPING\r\n$1\r\nx\r\n$4\r\ntest\r\n$14\r\n127.0.0.1:9999\r\n:0\r\n*0\r\n*0\r\n

// NodeFlags is a set of node state bits carried on the bus.
type NodeFlags uint16

const (
	// FlagHandshake: we've heard of this node but it hasn't answered us
	// yet, so we can't vouch for it. Gossip about it is ignored.
	FlagHandshake NodeFlags = 1 << iota
)

// Message is one message on the cluster bus.
type Message struct {
	Type   string
	ID     string
	Name   string
	Addr   string
	Flags  NodeFlags
	Slots  []SlotRange   // the sender's own slots
	Gossip []GossipEntry // other nodes the sender knows about
}

// GossipEntry is what the sender knows about one other node.
type GossipEntry struct {
	ID       string
	Name     string
	Addr     string
	Flags    NodeFlags
	PingSent int64 // Unix ms of the sender's unanswered PING to it, 0 if none
	PongRecv int64 // Unix ms of the last PONG the sender got from it, 0 if never
}

const (
	messageFields = 7
	gossipFields  = 6
)

func (m *Message) encode() []byte {
	slots := make([][]byte, 0, 2*len(m.Slots))
	for _, r := range m.Slots {
		slots = append(slots,
			core.EncodeInteger(int64(r.Start)),
			core.EncodeInteger(int64(r.End)))
	}
	gossip := make([][]byte, 0, len(m.Gossip))
	for _, g := range m.Gossip {
		gossip = append(gossip, core.EncodeRawArray([][]byte{
			core.EncodeBulkString(g.ID),
			core.EncodeBulkString(g.Name),
			core.EncodeBulkString(g.Addr),
			core.EncodeInteger(int64(g.Flags)),
			core.EncodeInteger(g.PingSent),
			core.EncodeInteger(g.PongRecv),
		}))
	}
	return core.EncodeRawArray([][]byte{
		core.EncodeBulkString(m.Type),
		core.EncodeBulkString(m.ID),
		core.EncodeBulkString(m.Name),
		core.EncodeBulkString(m.Addr),
		core.EncodeInteger(int64(m.Flags)),
		core.EncodeRawArray(slots),
		core.EncodeRawArray(gossip),
	})
}

// decodeMessage turns a decoded RESP value into a Message. Anything that
// doesn't match the format exactly is an error, and the caller drops the
// connection: a node that sends garbage shouldn't change our view of the
// cluster.
func decodeMessage(v any) (Message, error) {
	arr, ok := v.([]any)
	if !ok || len(arr) != messageFields {
		return Message{}, fmt.Errorf("cluster bus: want an array of %d, got %v", messageFields, v)
	}
	var m Message
	var err error
	if m.Type, err = wantString(arr[0], "type"); err != nil {
		return Message{}, err
	}
	if m.Type != "PING" && m.Type != "PONG" && m.Type != "MEET" {
		return Message{}, fmt.Errorf("cluster bus: unknown message type %q", m.Type)
	}
	if m.ID, err = wantString(arr[1], "id"); err != nil {
		return Message{}, err
	}
	if m.Name, err = wantString(arr[2], "name"); err != nil {
		return Message{}, err
	}
	if m.Addr, err = wantString(arr[3], "addr"); err != nil {
		return Message{}, err
	}
	if m.Flags, err = wantFlags(arr[4], "flags"); err != nil {
		return Message{}, err
	}
	if m.Slots, err = decodeSlots(arr[5]); err != nil {
		return Message{}, err
	}
	if m.Gossip, err = decodeGossip(arr[6]); err != nil {
		return Message{}, err
	}
	return m, nil
}

// decodeSlots reads the flat start/end list. Each range must be valid on
// its own; overlaps between ranges are the receiver's business.
func decodeSlots(v any) ([]SlotRange, error) {
	arr, ok := v.([]any)
	if !ok {
		return nil, fmt.Errorf("cluster bus: slots is not an array")
	}
	if len(arr)%2 != 0 {
		return nil, fmt.Errorf("cluster bus: slots has %d numbers, want start/end pairs", len(arr))
	}
	var out []SlotRange
	for i := 0; i < len(arr); i += 2 {
		start, err := wantInt(arr[i], "slot start")
		if err != nil {
			return nil, err
		}
		end, err := wantInt(arr[i+1], "slot end")
		if err != nil {
			return nil, err
		}
		if start < 0 || end >= NumSlots || start > end {
			return nil, fmt.Errorf("cluster bus: bad slot range %d-%d", start, end)
		}
		out = append(out, SlotRange{Start: int(start), End: int(end)})
	}
	return out, nil
}

func decodeGossip(v any) ([]GossipEntry, error) {
	arr, ok := v.([]any)
	if !ok {
		return nil, fmt.Errorf("cluster bus: gossip is not an array")
	}
	var out []GossipEntry
	for i, x := range arr {
		e, ok := x.([]any)
		if !ok || len(e) != gossipFields {
			return nil, fmt.Errorf("cluster bus: gossip entry %d: want an array of %d", i, gossipFields)
		}
		var g GossipEntry
		var err error
		if g.ID, err = wantString(e[0], "gossip id"); err != nil {
			return nil, err
		}
		if g.Name, err = wantString(e[1], "gossip name"); err != nil {
			return nil, err
		}
		if g.Addr, err = wantString(e[2], "gossip addr"); err != nil {
			return nil, err
		}
		if g.Flags, err = wantFlags(e[3], "gossip flags"); err != nil {
			return nil, err
		}
		if g.PingSent, err = wantTime(e[4], "gossip ping-sent"); err != nil {
			return nil, err
		}
		if g.PongRecv, err = wantTime(e[5], "gossip pong-recv"); err != nil {
			return nil, err
		}
		out = append(out, g)
	}
	return out, nil
}

func wantString(v any, what string) (string, error) {
	s, ok := v.(string)
	if !ok {
		return "", fmt.Errorf("cluster bus: %s is not a string", what)
	}
	return s, nil
}

func wantInt(v any, what string) (int64, error) {
	n, ok := v.(int64)
	if !ok {
		return 0, fmt.Errorf("cluster bus: %s is not an integer", what)
	}
	return n, nil
}

func wantFlags(v any, what string) (NodeFlags, error) {
	n, err := wantInt(v, what)
	if err != nil {
		return 0, err
	}
	if n < 0 || n > 0xFFFF {
		return 0, fmt.Errorf("cluster bus: %s %d out of range", what, n)
	}
	return NodeFlags(n), nil
}

func wantTime(v any, what string) (int64, error) {
	n, err := wantInt(v, what)
	if err != nil {
		return 0, err
	}
	if n < 0 {
		return 0, fmt.Errorf("cluster bus: %s is negative", what)
	}
	return n, nil
}
