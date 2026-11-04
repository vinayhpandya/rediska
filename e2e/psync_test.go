package e2e

import (
	"fmt"
	"strings"
	"testing"
)

// The 3 masters and 3 replicas these tests use: node4 replicates node1,
// node5 node2 and node6 node3.
var withReplicas = []string{
	"0-5460", "5461-10922", "10923-16383",
	"replicaof node1", "replicaof node2", "replicaof node3",
}

// TestPartialResync checks that a replica that loses its link for a
// moment gets just the changes it missed, and that a restarted master
// means a full copy.
func TestPartialResync(t *testing.T) {
	c := startClusterWith(t, withReplicas)
	const node4 = 3
	pair := [][2]int{{node1, node4}}

	// Keys tagged {age} are all in slot 741, node1's.
	for i := range 100 {
		expect(t, "SET", c.dial(t, node1).do("SET", fmt.Sprintf("{age}.%d", i), "v"), "OK")
	}
	c.waitInSync(t, pair)

	t.Run("a short disconnect continues where it left off", func(t *testing.T) {
		expect(t, "DEBUG RECONNECT", c.dial(t, node4).do("DEBUG", "RECONNECT"), "OK")
		// node4 reconnects after a second; these happen while it's away.
		for i := range 50 {
			expect(t, "SET", c.dial(t, node1).do("SET", fmt.Sprintf("{age}.missed.%d", i), "v"), "OK")
		}
		c.waitInSync(t, pair)

		info := c.dial(t, node1).do("INFO", "replication")
		for _, want := range []string{"sync_full:1", "sync_partial_ok:1"} {
			if !strings.Contains(info, want) {
				t.Errorf("INFO replication on node1 is missing %q:\n%s", want, info)
			}
		}
		masterOffset := infoField(info, "master_repl_offset")
		if got := infoField(c.dial(t, node4).do("INFO", "replication"), "slave_repl_offset"); got != masterOffset {
			t.Errorf("node4 is at change %s, node1 at %s", got, masterOffset)
		}
	})

	t.Run("a restarted master means a full copy", func(t *testing.T) {
		c.killNode(t, node1)
		c.startNode(t, node1) // a new process: a new replication ID, and no data
		c.waitFor(t, node4, "the link to the restarted master", func(cl *client) bool {
			return strings.Contains(cl.do("INFO", "replication"), "master_link_status:up")
		})
		c.waitInSync(t, pair)
		info := c.dial(t, node1).do("INFO", "replication")
		if !strings.Contains(info, "sync_full:1") || !strings.Contains(info, "sync_partial_ok:0") {
			t.Errorf("after the restart, INFO replication on node1:\n%s", info)
		}
	})
}

// With a backlog too small to hold what the replica missed, it gets a
// full copy instead.
func TestTinyBacklogMeansFullCopy(t *testing.T) {
	c := startClusterWith(t, withReplicas, "--repl-backlog-size", "100")
	const node4 = 3
	pair := [][2]int{{node1, node4}}
	expect(t, "SET", c.dial(t, node1).do("SET", "{age}.first", "v"), "OK")
	c.waitInSync(t, pair)

	expect(t, "DEBUG RECONNECT", c.dial(t, node4).do("DEBUG", "RECONNECT"), "OK")
	for i := range 50 { // about 2 KB of changes: far more than 100 bytes
		expect(t, "SET", c.dial(t, node1).do("SET", fmt.Sprintf("{age}.missed.%d", i), "v"), "OK")
	}
	c.waitInSync(t, pair)

	info := c.dial(t, node1).do("INFO", "replication")
	if !strings.Contains(info, "sync_full:2") || !strings.Contains(info, "sync_partial_ok:0") {
		t.Errorf("want 2 full copies and no partial resync; INFO replication on node1:\n%s", info)
	}
}

// infoField returns the value of name in an INFO reply, or "".
func infoField(info, name string) string {
	for _, line := range strings.Split(info, "\n") {
		if v, ok := strings.CutPrefix(strings.TrimSpace(line), name+":"); ok {
			return v
		}
	}
	return ""
}
