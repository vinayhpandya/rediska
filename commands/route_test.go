package commands

import (
	"fmt"
	"strings"
	"testing"

	"github.com/vinayhpandya/rediska/cluster"
)

// testLayout: I am node1 (0-8000), node2 owns 8001-16000, and nobody owns
// 16001-16383.
func useTestLayout(t *testing.T) {
	t.Helper()
	conf := "node1 127.0.0.1:7001 0-8000\nnode2 127.0.0.1:7002 8001-16000\n"
	l, err := cluster.ParseLayout(strings.NewReader(conf), 7001)
	if err != nil {
		t.Fatal(err)
	}
	cluster.Current = l
	t.Cleanup(func() { cluster.Current = nil })
}

func cmd(parts ...string) *Command {
	return &Command{Name: strings.ToUpper(parts[0]), Args: parts[1:]}
}

// keyInSlotRange finds a key whose slot is within [lo, hi].
func keyInSlotRange(t *testing.T, lo, hi int) (string, int) {
	t.Helper()
	for i := 0; i < 100000; i++ {
		k := fmt.Sprintf("key%d", i)
		if s := cluster.KeyHashSlot(k); s >= lo && s <= hi {
			return k, s
		}
	}
	t.Fatalf("no key found in slots %d-%d", lo, hi)
	return "", 0
}

func TestCheckClusterOwnedHere(t *testing.T) {
	useTestLayout(t)
	k, _ := keyInSlotRange(t, 0, 8000)
	if r := checkCluster(cmd("SET", k, "v")); r != nil {
		t.Fatalf("want nil, got %q", r)
	}
}

func TestCheckClusterMoved(t *testing.T) {
	useTestLayout(t)
	k, slot := keyInSlotRange(t, 8001, 16000)
	want := fmt.Sprintf("-MOVED %d 127.0.0.1:7002\r\n", slot)
	if r := string(checkCluster(cmd("GET", k))); r != want {
		t.Fatalf("want %q, got %q", want, r)
	}
}

func TestCheckClusterCrossSlot(t *testing.T) {
	useTestLayout(t)
	a, _ := keyInSlotRange(t, 0, 4000)
	b, _ := keyInSlotRange(t, 4001, 8000) // both mine, but different slots
	r := string(checkCluster(cmd("DEL", a, b)))
	if !strings.HasPrefix(r, "-CROSSSLOT") {
		t.Fatalf("want CROSSSLOT, got %q", r)
	}
}

func TestCheckClusterHashTags(t *testing.T) {
	useTestLayout(t)
	// {user1} is slot 8106, owned by node2: same slot, so MOVED, not CROSSSLOT.
	r := string(checkCluster(cmd("DEL", "{user1}.name", "{user1}.email")))
	if r != "-MOVED 8106 127.0.0.1:7002\r\n" {
		t.Fatalf("got %q", r)
	}
}

func TestCheckClusterUnowned(t *testing.T) {
	useTestLayout(t)
	k, _ := keyInSlotRange(t, 16001, 16383)
	if r := string(checkCluster(cmd("GET", k))); r != "-CLUSTERDOWN Hash slot not served\r\n" {
		t.Fatalf("got %q", r)
	}
}

func TestCheckClusterKeyless(t *testing.T) {
	useTestLayout(t)
	for _, c := range []*Command{cmd("PING"), cmd("INFO"), cmd("CLUSTER", "KEYSLOT", "foo"), cmd("GET")} {
		if r := checkCluster(c); r != nil {
			t.Errorf("%s: want nil, got %q", c.Name, r)
		}
	}
}

func TestCheckClusterDisabled(t *testing.T) {
	cluster.Current = nil
	if r := checkCluster(cmd("DEL", "a", "b", "c")); r != nil {
		t.Fatalf("cluster off: want nil, got %q", r)
	}
}

func TestCheckClusterBatch(t *testing.T) {
	useTestLayout(t)
	a, _ := keyInSlotRange(t, 0, 4000)
	b, _ := keyInSlotRange(t, 4001, 8000) // both mine, different slots

	// Same slot across commands: allowed.
	same := []*Command{cmd("SET", a, "1"), cmd("GET", a), cmd("PING")}
	if r := checkClusterBatch(same); r != nil {
		t.Errorf("same slot: want nil, got %q", r)
	}
	// Each command is fine alone, but together they span two slots.
	mixed := []*Command{cmd("SET", a, "1"), cmd("SET", b, "2")}
	if r := string(checkClusterBatch(mixed)); !strings.HasPrefix(r, "-CROSSSLOT") {
		t.Errorf("mixed slots: want CROSSSLOT, got %q", r)
	}
	// Only keyless commands: allowed.
	if r := checkClusterBatch([]*Command{cmd("PING"), cmd("INFO")}); r != nil {
		t.Errorf("keyless: want nil, got %q", r)
	}
}
