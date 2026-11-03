package e2e

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// TestReplicaSync starts 3 masters and 3 replicas and checks that each
// replica holds exactly its master's data (same DEBUG DIGEST and key
// count): after writes, deletes and expiry, and after the replica is
// restarted and copies everything again.
func TestReplicaSync(t *testing.T) {
	c := startClusterWith(t, []string{
		"0-5460", "5461-10922", "10923-16383",
		"replicaof node1", "replicaof node2", "replicaof node3",
	})
	const node4 = 3
	pairs := [][2]int{{node1, 3}, {node2, 4}, {node3, 5}} // master, replica

	// Before any data exists: an empty master still has to answer SYNC, so
	// every replica's link comes up.
	for _, p := range pairs {
		c.waitFor(t, p[1], "the link to an empty master to come up", func(cl *client) bool {
			return strings.Contains(cl.do("INFO", "replication"), "master_link_status:up")
		})
	}

	for i := range 300 {
		c.setOnOwner(t, fmt.Sprintf("key:%d", i), fmt.Sprint(i))
	}
	for i := range 20 {
		c.doOnOwner(t, "DEL", fmt.Sprintf("key:%d", i))
		c.doOnOwner(t, "SET", fmt.Sprintf("short:%d", i), "x", "PX", "300")
		c.doOnOwner(t, "INCR", fmt.Sprintf("counter:%d", i%5))
		c.doOnOwner(t, "PFADD", "{hll}", fmt.Sprint(i))
	}
	time.Sleep(500 * time.Millisecond) // let the short keys expire
	c.waitInSync(t, pairs)

	t.Run("INFO replication", func(t *testing.T) {
		info := c.dial(t, node4).do("INFO", "replication")
		for _, want := range []string{"role:slave", "master_port:" + strings.Split(c.addrs[node1], ":")[1],
			"master_link_status:up"} {
			if !strings.Contains(info, want) {
				t.Errorf("INFO replication on node4 is missing %q:\n%s", want, info)
			}
		}
		info = c.dial(t, node1).do("INFO", "replication")
		if !strings.Contains(info, "role:master") || !strings.Contains(info, "connected_slaves:1") {
			t.Errorf("INFO replication on node1:\n%s", info)
		}
	})

	t.Run("a replica can't serve SYNC", func(t *testing.T) {
		expect(t, "SYNC on node4", c.dial(t, node4).do("SYNC"),
			"ERR:ERR this node is a replica; SYNC with its master instead")
	})

	t.Run("a restarted replica copies everything again", func(t *testing.T) {
		c.killNode(t, node4)
		for i := range 100 { // writes it misses while it's down
			c.setOnOwner(t, fmt.Sprintf("later:%d", i), "x")
		}
		c.startNode(t, node4)
		c.waitInSync(t, pairs)
	})
}

// doOnOwner runs a command on the node that owns its key (args[1]),
// following a MOVED reply from node1 if needed.
func (c *testCluster) doOnOwner(t *testing.T, args ...string) string {
	t.Helper()
	reply := c.dial(t, node1).do(args...)
	if !strings.HasPrefix(reply, "ERR:MOVED ") {
		return reply
	}
	addr := strings.Fields(reply)[2]
	for i, a := range c.addrs {
		if a == addr {
			return c.dial(t, i).do(args...)
		}
	}
	t.Fatalf("MOVED to %s, which isn't a node of this cluster", addr)
	return ""
}

func (c *testCluster) setOnOwner(t *testing.T, key, value string) {
	t.Helper()
	expect(t, "SET "+key, c.doOnOwner(t, "SET", key, value), "OK")
}

// waitInSync waits until every replica has exactly its master's data.
func (c *testCluster) waitInSync(t *testing.T, pairs [][2]int) {
	t.Helper()
	for _, p := range pairs {
		master, replica := p[0], p[1]
		var mDigest, rDigest, mKeys, rKeys string
		deadline := time.Now().Add(10 * time.Second)
		for {
			mDigest, mKeys = c.dial(t, master).do("DEBUG", "DIGEST"), c.dial(t, master).do("DEBUG", "KEYS")
			rDigest, rKeys = c.dial(t, replica).do("DEBUG", "DIGEST"), c.dial(t, replica).do("DEBUG", "KEYS")
			if mDigest == rDigest && mKeys == rKeys {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("node%d never caught up with node%d: %s keys (digest %s) vs %s keys (digest %s)\nreplica log:\n%s",
					replica+1, master+1, rKeys, rDigest, mKeys, mDigest, c.log(replica))
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
}
