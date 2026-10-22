package commands

import (
	"fmt"
	"strings"
	"testing"

	"github.com/vinayhpandya/rediska/cluster"
	"github.com/vinayhpandya/rediska/config"
)

// useThreeNodes loads the same layout as cluster.conf on your Mac, as node1.
// node3's slots are listed before node2's on purpose, to check that
// CLUSTER SLOTS sorts by slot rather than following the file.
func useThreeNodes(t *testing.T) *cluster.Layout {
	t.Helper()
	conf := "node1 127.0.0.1:7001 0-5460\n" +
		"node3 127.0.0.1:7003 10923-16383\n" +
		"node2 127.0.0.1:7002 5461-10921 10922\n" // a range plus a single slot
	l, err := cluster.ParseLayout(strings.NewReader(conf), 7001)
	if err != nil {
		t.Fatal(err)
	}
	cluster.Current, config.ClusterEnabled = l, true
	t.Cleanup(func() { cluster.Current, config.ClusterEnabled = nil, false })
	return l
}

func id(l *cluster.Layout, name string) string {
	for _, n := range l.Nodes {
		if n.Name == name {
			return n.ID
		}
	}
	return ""
}

func TestClusterMyID(t *testing.T) {
	l := useThreeNodes(t)
	want := "$40\r\n" + id(l, "node1") + "\r\n"
	if got := string(handleCluster([]string{"myid"})); got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestClusterSlots(t *testing.T) {
	l := useThreeNodes(t)
	entry := func(start, end, port int, name string) string {
		return fmt.Sprintf("*3\r\n:%d\r\n:%d\r\n*4\r\n$9\r\n127.0.0.1\r\n:%d\r\n$40\r\n%s\r\n*0\r\n",
			start, end, port, id(l, name))
	}
	want := "*4\r\n" + // four ranges, sorted by start slot
		entry(0, 5460, 7001, "node1") +
		entry(5461, 10921, 7002, "node2") +
		entry(10922, 10922, 7002, "node2") +
		entry(10923, 16383, 7003, "node3")
	if got := string(handleCluster([]string{"SLOTS"})); got != want {
		t.Fatalf("got\n%q\nwant\n%q", got, want)
	}
}

func TestClusterNodes(t *testing.T) {
	l := useThreeNodes(t)
	body := id(l, "node1") + " 127.0.0.1:7001@17001 myself,master - 0 0 1 connected 0-5460\n" +
		id(l, "node3") + " 127.0.0.1:7003@17003 master - 0 0 2 connected 10923-16383\n" +
		id(l, "node2") + " 127.0.0.1:7002@17002 master - 0 0 3 connected 5461-10921 10922\n"
	want := fmt.Sprintf("$%d\r\n%s\r\n", len(body), body)
	if got := string(handleCluster([]string{"nodes"})); got != want {
		t.Fatalf("got\n%s\nwant\n%s", got, want)
	}
}

func TestClusterInfo(t *testing.T) {
	useThreeNodes(t)
	got := string(handleCluster([]string{"info"}))
	for _, line := range []string{"cluster_state:ok\r\n", "cluster_slots_assigned:16384\r\n",
		"cluster_known_nodes:3\r\n", "cluster_size:3\r\n", "cluster_my_epoch:1\r\n"} {
		if !strings.Contains(got, line) {
			t.Errorf("missing %q in %q", line, got)
		}
	}
}

func TestClusterInfoNotFullyCovered(t *testing.T) {
	l, _ := cluster.ParseLayout(strings.NewReader("node1 127.0.0.1:7001 0-100"), 7001)
	cluster.Current, config.ClusterEnabled = l, true
	t.Cleanup(func() { cluster.Current, config.ClusterEnabled = nil, false })
	got := string(handleCluster([]string{"info"}))
	if !strings.Contains(got, "cluster_state:fail\r\n") || !strings.Contains(got, "cluster_slots_assigned:101\r\n") {
		t.Fatalf("got %q", got)
	}
}

func TestClusterErrors(t *testing.T) {
	useThreeNodes(t)
	cases := map[string][]string{
		"-ERR wrong number of arguments for 'cluster|slots' command\r\n": {"slots", "x"},
		"-ERR wrong number of arguments for 'cluster|myid' command\r\n":  {"MYID", "x"},
		"-ERR unknown subcommand 'foo'. Try CLUSTER HELP.\r\n":           {"foo"},
	}
	for want, args := range cases {
		if got := string(handleCluster(args)); got != want {
			t.Errorf("%v: got %q, want %q", args, got, want)
		}
	}
}

func TestInfoClusterSection(t *testing.T) {
	config.ClusterEnabled = false
	if got := string(handleInfo([]string{"cluster"})); !strings.Contains(got, "cluster_enabled:0\r\n") {
		t.Errorf("off: got %q", got)
	}
	useThreeNodes(t)
	if got := string(handleInfo([]string{"cluster"})); !strings.Contains(got, "# Cluster\r\ncluster_enabled:1\r\n") {
		t.Errorf("on: got %q", got)
	}
	all := string(handleInfo(nil))
	if !strings.Contains(all, "# Cluster") || !strings.Contains(all, "# Keyspace") {
		t.Errorf("all: got %q", all)
	}
}
