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
	// MasterID is the ID of the master this node replicates, or "" if the
	// node is a master itself. A replica owns no slots: clients that send
	// it a key get MOVED to the slot's owner, which is its master.
	MasterID string
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

// IsReplica reports whether n replicates another node.
func (n *Node) IsReplica() bool { return n.MasterID != "" }

// ============================================================ runtime changes
// The layout starts from cluster.conf, then the cluster bus changes it as
// nodes learn about each other. Only dispatchWorker calls these, so routing
// (which also runs there) always sees a consistent map, without locks.

// AddNode adds a node learned while running. It owns no slots until it
// announces some (see SetSlots).
func (l *Layout) AddNode(n *Node) {
	l.Nodes = append(l.Nodes, n)
}

// RemoveNode forgets n, and frees the slots it owned.
func (l *Layout) RemoveNode(n *Node) {
	l.SetSlots(n, nil)
	for i, m := range l.Nodes {
		if m == n {
			l.Nodes = append(l.Nodes[:i:i], l.Nodes[i+1:]...)
			return
		}
	}
}

// ForgetOthersSlots keeps only Self's slots, and clears every other node's.
// The server calls it at startup in cluster mode, so that each node's slots
// come from that node itself, over the bus, instead of from our copy of the
// file. A node only ever speaks for its own slots.
func (l *Layout) ForgetOthersSlots() {
	for _, n := range l.Nodes {
		if n != l.Self {
			l.SetSlots(n, nil)
		}
	}
}

// ForgetOthersRoles marks every node but Self as a master, for the same
// reason as ForgetOthersSlots: each node tells us its own role over the
// bus. cluster.conf only decides Self's role.
func (l *Layout) ForgetOthersRoles() {
	for _, n := range l.Nodes {
		if n != l.Self {
			n.MasterID = ""
		}
	}
}

// SetSlots records the slots that n says it owns. n gives its whole set
// each time, so slots it no longer lists are released.
//
// Until slots carry config epochs, the first owner wins: a slot that
// another node already owns stays with that node. SetSlots returns the
// slots it refused, so the caller can log them. n.Slots ends up as the
// slots n was actually given. Never call it for Self: a node's own slots
// change only through its own config.
func (l *Layout) SetSlots(n *Node, announced []SlotRange) (refused []SlotRange) {
	for s := range l.owner {
		if l.owner[s] == n {
			l.owner[s] = nil
		}
	}
	for _, r := range announced {
		for s := r.Start; s <= r.End; s++ {
			if l.owner[s] == nil {
				l.owner[s] = n
			} else {
				refused = appendSlot(refused, s)
			}
		}
	}
	n.Slots = l.rangesOwnedBy(n)
	return refused
}

// rangesOwnedBy rebuilds n's slots as sorted ranges from the owner table.
func (l *Layout) rangesOwnedBy(n *Node) []SlotRange {
	var out []SlotRange
	for s := range l.owner {
		if l.owner[s] == n {
			out = appendSlot(out, s)
		}
	}
	return out
}

// appendSlot adds slot s to rs, extending the last range when s follows
// it. Slots must be added in increasing order.
func appendSlot(rs []SlotRange, s int) []SlotRange {
	if k := len(rs) - 1; k >= 0 && rs[k].End == s-1 {
		rs[k].End = s
		return rs
	}
	return append(rs, SlotRange{Start: s, End: s})
}

// LoadLayout reads the cluster layout file at path. myPort is this node's
// port, used to find which line of the file describes this node.
func LoadLayout(path string, myPort int, myName string) (*Layout, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("cluster config: %w", err)
	}
	defer f.Close()
	return ParseLayoutAs(f, myPort, myName)
}

func ParseLayout(r io.Reader, myPort int) (*Layout, error) {
	return ParseLayoutAs(r, myPort, "")
}

// ParseLayout parses a layout file. Each non-empty line is:
//
// or a replica of the master called <master>, which owns no slots:
//
//	<name> <host:port> replicaof <master>
//
// Anything after '#' is a comment. Any mistake is an error, because a node
// with a wrong layout would silently send clients to the wrong place.
func ParseLayoutAs(r io.Reader, myPort int, myName string) (*Layout, error) {
	l := &Layout{}
	names := map[string]bool{}
	addrs := map[string]bool{}
	replicaOf := map[*Node]string{}
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
			return nil, fmt.Errorf("cluster config line %d: want <name> <host:port> <slots...> or <name> <host:port> replicaof <master>", lineNo)
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
		specs := fields[2:]
		if strings.EqualFold(specs[0], "replicaof") {
			if len(specs) != 2 {
				return nil, fmt.Errorf("cluster config line %d: want replicaof <master>, and no slots", lineNo)
			}
			replicaOf[n] = specs[1]
			specs = nil
		}
		for _, spec := range specs {
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
		switch {
		case myName != "":
			if name == myName {
				l.Self = n // names are unique, checked above
			}
		case port == myPort:
			if l.Self != nil {
				return nil, fmt.Errorf("cluster config: port %d appears twice, can't tell which node is me (use --cluster-node-name)", port)
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
	if err := l.linkReplicas(replicaOf); err != nil {
		return nil, err
	}
	if l.Self == nil {
		if myName != "" {
			return nil, fmt.Errorf("cluster config: no node is named %q, so I can't find myself", myName)
		}
		return nil, fmt.Errorf("cluster config: no node uses port %d, so I can't find myself", myPort)
	}
	return l, nil
}

// linkReplicas sets each replica's MasterID, once every node is known (a
// replica may be listed before its master). A replica must follow a
// master: replicas of replicas aren't supported.
func (l *Layout) linkReplicas(replicaOf map[*Node]string) error {
	byName := map[string]*Node{}
	for _, n := range l.Nodes {
		byName[n.Name] = n
	}
	for _, n := range l.Nodes { // in file order, so errors are predictable
		masterName, ok := replicaOf[n]
		if !ok {
			continue
		}
		m := byName[masterName]
		switch {
		case m == nil:
			return fmt.Errorf("cluster config: %s is a replica of %q, which isn't in the file", n.Name, masterName)
		case m == n:
			return fmt.Errorf("cluster config: %s can't be a replica of itself", n.Name)
		case replicaOf[m] != "":
			return fmt.Errorf("cluster config: %s is a replica of %s, which is a replica itself", n.Name, m.Name)
		}
		n.MasterID = m.ID
	}
	return nil
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
