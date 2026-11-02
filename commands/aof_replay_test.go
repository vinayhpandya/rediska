package commands

import (
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/vinayhpandya/rediska/core"
	"github.com/vinayhpandya/rediska/store"
)

// aofNode is one node's store and AOF, which a test can restart.
type aofNode struct {
	t    *testing.T
	path string
	aof  *core.AOF
}

// newAOFNode gives the test a fresh store.Default writing to a new AOF in
// a temporary directory.
func newAOFNode(t *testing.T) *aofNode {
	t.Helper()
	oldStore := store.Default
	t.Cleanup(func() {
		store.Default = oldStore
		SetAOF(nil)
	})
	n := &aofNode{t: t, path: filepath.Join(t.TempDir(), "appendonly.aof")}
	n.open()
	return n
}

func (n *aofNode) open() {
	n.t.Helper()
	a, err := core.NewAOF(n.path)
	if err != nil {
		n.t.Fatal(err)
	}
	n.aof = a
	store.Default = store.New()
}

// do runs one command the way a client's command would be run.
func (n *aofNode) do(args ...string) string {
	n.t.Helper()
	return string(Dispatch(&Command{Name: strings.ToUpper(args[0]), Args: args[1:]}))
}

// restart closes the AOF and starts again from an empty store, replaying
// the file exactly as server.Run does at startup: AOF logging is off while
// replaying, and turned on afterwards.
func (n *aofNode) restart() {
	n.t.Helper()
	SetAOF(nil)
	if err := n.aof.Sync(); err != nil {
		n.t.Fatal(err)
	}
	if err := n.aof.Close(); err != nil {
		n.t.Fatal(err)
	}
	n.open()
	err := n.aof.Recover(func(args []string) {
		if len(args) > 0 {
			Replay(&Command{Name: strings.ToUpper(args[0]), Args: args[1:]})
		}
	})
	if err != nil {
		n.t.Fatal(err)
	}
	SetAOF(n.aof)
}

// A key that expired before the restart must still be gone after it.
func TestAOFExpiredKeyStaysExpiredAfterRestart(t *testing.T) {
	n := newAOFNode(t)
	SetAOF(n.aof)

	if r := n.do("SET", "session", "abc", "PX", "200"); r != "+OK\r\n" {
		t.Fatalf("SET: got %q", r)
	}
	time.Sleep(300 * time.Millisecond)
	if r := n.do("GET", "session"); r != "$-1\r\n" {
		t.Fatalf("GET before restart: got %q, want nil (the key has expired)", r)
	}

	n.restart()

	if v, ok := store.Default.Get("session"); ok {
		t.Fatalf("after restart, session = %q; it expired before the restart and must stay gone", v)
	}
}

// EXPIRE sets a deadline. A restart must not move that deadline later.
func TestAOFExpireDeadlineSurvivesRestart(t *testing.T) {
	n := newAOFNode(t)
	SetAOF(n.aof)

	if r := n.do("SET", "k", "v"); r != "+OK\r\n" {
		t.Fatalf("SET: got %q", r)
	}
	if r := n.do("EXPIRE", "k", "2"); r != ":1\r\n" {
		t.Fatalf("EXPIRE: got %q", r)
	}
	// Don't read k while waiting: a read would expire it lazily, and that
	// is what the test above covers. This one is only about the deadline.
	time.Sleep(2200 * time.Millisecond)

	n.restart()

	if v, ok := store.Default.Get("k"); ok {
		t.Fatalf("after restart, k = %q with TTL %d s; its 2 s deadline passed before the restart",
			v, store.Default.TTL("k"))
	}
}
