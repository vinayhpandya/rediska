package e2e

import (
	"strings"
	"testing"
	"time"
)

// TestFailureDetection crashes node3 in a real 3-node cluster and checks
// what clients see, then brings it back. The node timeout is 1 s instead of
// the default 15 s, so the whole test takes a few seconds.
//
// The flow it checks (see docs/cluster.md, "Failure detection"):
//
//	node3 crashes → node1 and node2 suspect it (PFAIL) → they see each
//	other's suspicion in gossip → 2 of 3 masters agree → FAIL →
//	node3's slots get CLUSTERDOWN, everything else keeps working →
//	node3 restarts and answers → FAIL is cleared
func TestFailureDetection(t *testing.T) {
	c := startCluster(t, "--cluster-node-timeout", "1000")

	// One key on each side: age is node1's (slot 741), visits node3's (12263).
	expect(t, "SET age", c.dial(t, node1).do("SET", "age", "30"), "OK")
	expect(t, "SET visits", c.dial(t, node3).do("SET", "visits", "1"), "OK")

	c.killNode(t, node3)

	// Both survivors must reach FAIL (not just fail?) on their own or
	// through the FAIL broadcast.
	for _, node := range []int{node1, node2} {
		c.waitFor(t, node, "node3 marked FAIL", func(cl *client) bool {
			return nodeFlags(cl, c.addrs[node3]) == "master,fail"
		})
	}

	t.Run("the failed node's slots get CLUSTERDOWN", func(t *testing.T) {
		expect(t, "GET visits on node1", c.dial(t, node1).do("GET", "visits"),
			"ERR:CLUSTERDOWN Hash slot not served")
		expect(t, "GET visits on node2", c.dial(t, node2).do("GET", "visits"),
			"ERR:CLUSTERDOWN Hash slot not served")
	})

	t.Run("every other slot keeps working", func(t *testing.T) {
		n1 := c.dial(t, node1)
		expect(t, "GET age on node1", n1.do("GET", "age"), "30")
		expect(t, "SET name on node1", n1.do("SET", "name", "vinay"), "ERR:MOVED 5798 "+c.addrs[node2])
		expect(t, "SET name on node2", c.dial(t, node2).do("SET", "name", "vinay"), "OK")
	})

	t.Run("CLUSTER INFO reports the failure", func(t *testing.T) {
		info := c.dial(t, node1).do("CLUSTER", "INFO")
		for _, want := range []string{"cluster_state:fail", "cluster_slots_fail:5461", "cluster_slots_ok:10923"} {
			if !strings.Contains(info, want) {
				t.Errorf("CLUSTER INFO is missing %q:\n%s", want, info)
			}
		}
	})

	// node3 comes back (empty: the test runs without AOF). As soon as it
	// answers, FAIL is cleared and its slots are routed to it again.
	c.startNode(t, node3)
	for _, node := range []int{node1, node2} {
		c.waitFor(t, node, "FAIL cleared", func(cl *client) bool {
			return nodeFlags(cl, c.addrs[node3]) == "master" &&
				strings.Contains(cl.do("CLUSTER", "INFO"), "cluster_state:ok")
		})
	}

	t.Run("after recovery, node3's slots are routed to it", func(t *testing.T) {
		expect(t, "GET visits on node1", c.dial(t, node1).do("GET", "visits"),
			"ERR:MOVED 12263 "+c.addrs[node3])
		expect(t, "SET visits on node3", c.dial(t, node3).do("SET", "visits", "2"), "OK")
	})
}

// waitFor polls node until ok returns true, for up to 10 s, with a fresh
// connection each time (a client's connection has a 5 s deadline).
func (c *testCluster) waitFor(t *testing.T, node int, what string, ok func(*client) bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		cl := c.dial(t, node)
		if ok(cl) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("node%d: timed out waiting for: %s\nCLUSTER NODES:\n%s\nlog:\n%s",
				node+1, what, cl.do("CLUSTER", "NODES"), c.log(node))
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// nodeFlags returns the flags column (e.g. "master,fail?") of the line in
// cl's CLUSTER NODES for the node at addr, or "" if there is none.
//
//	<id> <ip:port@busport> <flags> <master> <ping-sent> <pong-recv> <epoch> <link> <slots...>
func nodeFlags(cl *client, addr string) string {
	for _, line := range strings.Split(cl.do("CLUSTER", "NODES"), "\n") {
		f := strings.Fields(line)
		if len(f) > 2 && strings.HasPrefix(f[1], addr+"@") {
			return f[2]
		}
	}
	return ""
}
