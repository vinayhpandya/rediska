package commands

import (
	"strings"
	"testing"

	"github.com/vinayhpandya/rediska/cluster"
	"github.com/vinayhpandya/rediska/config"
	"github.com/vinayhpandya/rediska/store"
)

// freshStore gives a test its own empty store.Default.
func freshStore(t *testing.T) {
	t.Helper()
	old := store.Default
	store.Default = store.New()
	t.Cleanup(func() { store.Default = old })
}

// AOF replay must load every key in the file, even ones this node no
// longer owns after cluster.conf changed.
func TestReplaySkipsClusterCheck(t *testing.T) {
	freshStore(t)
	// "name" is slot 5798; in this layout node2 owns it, and we are node1.
	l, err := cluster.ParseLayout(strings.NewReader(
		"node1 127.0.0.1:7001 0-5460\nnode2 127.0.0.1:7002 5461-16383\n"), 7001)
	if err != nil {
		t.Fatal(err)
	}
	cluster.Current = l
	t.Cleanup(func() { cluster.Current = nil })

	set := &Command{Name: "SET", Args: []string{"name", "vinay"}}
	if r := string(Dispatch(set)); !strings.HasPrefix(r, "-MOVED") {
		t.Fatalf("Dispatch should still redirect clients, got %q", r)
	}
	Replay(set)
	if v, ok := store.Default.Get("name"); !ok || v != "vinay" {
		t.Fatalf("after Replay: Get(name) = %q, %v; want the key loaded", v, ok)
	}
}

// A write rejected for OOM must have no effect.
func TestOOMRejectsBeforeRunning(t *testing.T) {
	freshStore(t)
	oldMax, oldPolicy := config.MaxMemory, config.MaxMemoryPolicy
	config.MaxMemory, config.MaxMemoryPolicy = 1, "noeviction" // 1 MB
	t.Cleanup(func() { config.MaxMemory, config.MaxMemoryPolicy = oldMax, oldPolicy })

	big := strings.Repeat("x", 1024)
	for i := 0; !store.Default.OverMemory(1 << 20); i++ {
		store.Default.Set("fill"+string(rune(i)), big, 0)
	}

	r := string(Dispatch(&Command{Name: "SET", Args: []string{"newkey", "v"}}))
	if !strings.HasPrefix(r, "-OOM") {
		t.Fatalf("SET over maxmemory: got %q, want OOM", r)
	}
	if _, ok := store.Default.Get("newkey"); ok {
		t.Fatal("the rejected SET was stored anyway")
	}
	// Commands that free memory still work, as in Redis.
	if r := string(Dispatch(&Command{Name: "DEL", Args: []string{"fill" + string(rune(0))}})); r != ":1\r\n" {
		t.Errorf("DEL over maxmemory: got %q, want :1", r)
	}
}

func TestPFAddWrongTypeReply(t *testing.T) {
	freshStore(t)
	Dispatch(&Command{Name: "SET", Args: []string{"name", "vinay"}})
	got := string(Dispatch(&Command{Name: "PFADD", Args: []string{"name", "a"}}))
	want := "-WRONGTYPE Key is not a valid HyperLogLog string value.\r\n"
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}
