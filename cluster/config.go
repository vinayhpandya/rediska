package cluster

import (
	"bufio"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
)

// Node is one member of the cluster, as described by one line of the
// cluster layout file.
type Node struct {
	Name  string
	Addr  string // "host:port"
	Port  int
	ID    string // 40 hex chars, derived from Name so it's stable across restarts
	Slots []SlotRange
}

// SlotRange is an inclusive range of slots, e.g. 0-5460.
type SlotRange struct {
	Start, End int
}

// Layout is the whole cluster as seen by this node.
type Layout struct {
	Nodes []*Node
	Self  *Node
	owner [NumSlots]*Node // owner[slot] = node that serves it, nil if nobody
}

// Current is the layout loaded at startup. It is nil when cluster mode is off.
var Current *Layout

// Owner returns the node that owns slot, or nil if nobody does.
func (l *Layout) Owner(slot int) *Node {
	if slot < 0 || slot >= NumSlots {
		return nil
	}
	return l.owner[slot]
}

// LoadLayout reads the cluster layout file at path. myPort is this node's
// port, used to find which line of the file describes this node.
func LoadLayout(path string, myPort int) (*Layout, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("cluster config: %w", err)
	}
	defer f.Close()
	return ParseLayout(f, myPort)
}

// ParseLayout parses a layout file. Each non-empty line is:
//
//	<name> <host:port> <slot or range> [<slot or range> ...]
//
// Anything after '#' is a comment. Any mistake is an error, because a node
// with a wrong layout would silently send clients to the wrong place.
func ParseLayout(r io.Reader, myPort int) (*Layout, error) {
	l := &Layout{}
	names := map[string]bool{}
	addrs := map[string]bool{}

	sc := bufio.NewScanner(r)
	lineNo := 0
	for sc.Scan() {
		lineNo++
		line := sc.Text()
		if i := strings.IndexByte(line, '#'); i >= 0 {
			line = line[:i]
		}
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		if len(fields) < 3 {
			return nil, fmt.Errorf("cluster config line %d: want <name> <host:port> <slots...>", lineNo)
		}

		name, addr := fields[0], fields[1]
		if names[name] {
			return nil, fmt.Errorf("cluster config line %d: duplicate node name %q", lineNo, name)
		}
		if addrs[addr] {
			return nil, fmt.Errorf("cluster config line %d: duplicate address %q", lineNo, addr)
		}
		_, portStr, err := net.SplitHostPort(addr)
		if err != nil {
			return nil, fmt.Errorf("cluster config line %d: bad address %q: %v", lineNo, addr, err)
		}
		port, err := strconv.Atoi(portStr)
		if err != nil {
			return nil, fmt.Errorf("cluster config line %d: bad port in %q", lineNo, addr)
		}

		n := &Node{Name: name, Addr: addr, Port: port, ID: nodeID(name)}
		for _, spec := range fields[2:] {
			sr, err := parseRange(spec)
			if err != nil {
				return nil, fmt.Errorf("cluster config line %d: %v", lineNo, err)
			}
			for s := sr.Start; s <= sr.End; s++ {
				if prev := l.owner[s]; prev != nil {
					return nil, fmt.Errorf("cluster config line %d: slot %d claimed by both %s and %s",
						lineNo, s, prev.Name, name)
				}
				l.owner[s] = n
			}
			n.Slots = append(n.Slots, sr)
		}

		names[name], addrs[addr] = true, true
		l.Nodes = append(l.Nodes, n)
		if port == myPort {
			if l.Self != nil {
				return nil, fmt.Errorf("cluster config: port %d appears twice, can't tell which node is me", port)
			}
			l.Self = n
		}
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("cluster config: %w", err)
	}
	if len(l.Nodes) == 0 {
		return nil, fmt.Errorf("cluster config: no nodes defined")
	}
	if l.Self == nil {
		return nil, fmt.Errorf("cluster config: no node uses port %d, so I can't find myself", myPort)
	}
	return l, nil
}

// parseRange accepts "42" or "0-5460".
func parseRange(spec string) (SlotRange, error) {
	lo, hi, isRange := strings.Cut(spec, "-")
	start, err := strconv.Atoi(lo)
	if err != nil {
		return SlotRange{}, fmt.Errorf("bad slot %q", spec)
	}
	end := start
	if isRange {
		if end, err = strconv.Atoi(hi); err != nil {
			return SlotRange{}, fmt.Errorf("bad slot range %q", spec)
		}
	}
	if start < 0 || end >= NumSlots || start > end {
		return SlotRange{}, fmt.Errorf("slot range %q must be within 0-%d, start <= end", spec, NumSlots-1)
	}
	return SlotRange{Start: start, End: end}, nil
}

func nodeID(name string) string {
	sum := sha1.Sum([]byte(name))
	return hex.EncodeToString(sum[:])
}
