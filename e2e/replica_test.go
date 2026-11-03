package e2e

import (
	"strings"
	"testing"
)

// TestReplicaRoles starts 3 masters and 3 replicas (node4 replicates
// node1, node5 node2, node6 node3) and checks that every node learns every
// role over the cluster bus, and that replicas send clients to the owner.
func TestReplicaRoles(t *testing.T) {
	c := startClusterWith(t, []string{
		"0-5460", "5461-10922", "10923-16383",
		"replicaof node1", "replicaof node2", "replicaof node3",
	})
	const node4, node5, node6 = 3, 4, 5

	ids := make([]string, len(c.addrs))
	for i := range c.addrs {
		ids[i] = c.dial(t, i).do("CLUSTER", "MYID")
	}
	masterOf := map[int]int{node4: node1, node5: node2, node6: node3}

	// What every node should eventually show for node n: its flags without
	// "myself," and its master column.
	want := func(n int) (role, master string) {
		if m, ok := masterOf[n]; ok {
			return "slave", ids[m]
		}
		return "master", "-"
	}

	for viewer := range c.addrs {
		c.waitFor(t, viewer, "every node's role", func(cl *client) bool {
			nodes := cl.do("CLUSTER", "NODES")
			for n, addr := range c.addrs {
				role, master := want(n)
				f := nodeLine(nodes, addr)
				if len(f) < 4 || strings.TrimPrefix(f[2], "myself,") != role || f[3] != master {
					return false
				}
			}
			return true
		})
	}

	t.Run("replicas redirect to the slot's owner", func(t *testing.T) {
		// age is in slot 741 (node1), visits in 12263 (node3).
		expect(t, "SET age on node4", c.dial(t, node4).do("SET", "age", "30"),
			"ERR:MOVED 741 "+c.addrs[node1])
		expect(t, "GET age on node4", c.dial(t, node4).do("GET", "age"),
			"ERR:MOVED 741 "+c.addrs[node1])
		expect(t, "SET visits on node4", c.dial(t, node4).do("SET", "visits", "1"),
			"ERR:MOVED 12263 "+c.addrs[node3])
	})

	t.Run("CLUSTER INFO counts only masters", func(t *testing.T) {
		info := c.dial(t, node4).do("CLUSTER", "INFO")
		for _, w := range []string{"cluster_state:ok", "cluster_known_nodes:6", "cluster_size:3"} {
			if !strings.Contains(info, w) {
				t.Errorf("CLUSTER INFO on node4 is missing %q:\n%s", w, info)
			}
		}
	})
}

// nodeLine returns the fields of the CLUSTER NODES line for addr, or nil.
func nodeLine(nodes, addr string) []string {
	for _, line := range strings.Split(nodes, "\n") {
		f := strings.Fields(line)
		if len(f) > 1 && strings.HasPrefix(f[1], addr+"@") {
			return f
		}
	}
	return nil
}
