package server

import (
	"strings"
	"testing"

	"github.com/vinayhpandya/rediska/cluster"
	"github.com/vinayhpandya/rediska/commands"
)

// Same layout as the 3-node cluster on your Mac; this node is node1 (7001).
// Slots used below: age=741 (mine), foo2=1044 (mine),
// name=5798 (node2), {user1}=8106 (node2).
func useTestLayout(t *testing.T) {
	t.Helper()
	conf := "node1 127.0.0.1:7001 0-5460\n" +
		"node2 127.0.0.1:7002 5461-10922\n" +
		"node3 127.0.0.1:7003 10923-16383\n"
	l, err := cluster.ParseLayout(strings.NewReader(conf), 7001)
	if err != nil {
		t.Fatal(err)
	}
	cluster.Current = l
	t.Cleanup(func() { cluster.Current = nil })
}

// runTx sends each line through txState like a connection would, and returns
// every reply as a string. EXEC's batch is run with DispatchBatch, like the
// dispatch worker does.
func runTx(t *testing.T, lines ...string) []string {
	t.Helper()
	tx := newTxState()
	var out []string
	for _, line := range lines {
		parts := strings.Fields(line)
		c := &commands.Command{Name: strings.ToUpper(parts[0]), Args: parts[1:]}
		action, reply, batch := tx.handle(c)
		switch action {
		case actionReply:
			out = append(out, string(reply))
		case actionExec:
			out = append(out, string(commands.DispatchBatch(batch)))
		default:
			out = append(out, string(commands.Dispatch(c)))
		}
	}
	return out
}

const (
	ok        = "+OK\r\n"
	queued    = "+QUEUED\r\n"
	execAbort = "-EXECABORT Transaction discarded because of previous errors.\r\n"
	crossSlot = "-CROSSSLOT Keys in request don't hash to the same slot\r\n"
)

func expect(t *testing.T, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %d replies %q, want %d %q", len(got), got, len(want), want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("reply %d: got %q, want %q", i, got[i], want[i])
		}
	}
}

// Each case below was run against a real Redis 7 cluster first; the wanted
// replies are what real Redis sent.

func TestTxSlotOwnedElsewhere(t *testing.T) {
	useTestLayout(t)
	got := runTx(t, "MULTI", "SET {user1}.name vinay", "SET {user1}.email x", "EXEC")
	expect(t, got, []string{ok,
		"-MOVED 8106 127.0.0.1:7002\r\n",
		"-MOVED 8106 127.0.0.1:7002\r\n",
		execAbort})
}

func TestTxOneCommandMoved(t *testing.T) {
	useTestLayout(t)
	got := runTx(t, "MULTI", "SET age 30", "SET name vinay", "EXEC", "GET age")
	expect(t, got, []string{ok, queued,
		"-MOVED 5798 127.0.0.1:7002\r\n",
		execAbort,
		"$-1\r\n"}) // nothing ran: age is still unset
}

func TestTxBothMineDifferentSlots(t *testing.T) {
	useTestLayout(t)
	got := runTx(t, "MULTI", "SET age 30", "SET foo2 x", "EXEC", "GET age", "GET foo2")
	expect(t, got, []string{ok, queued, queued, crossSlot, "$-1\r\n", "$-1\r\n"})
}

func TestTxCrossSlotInsideOneCommand(t *testing.T) {
	useTestLayout(t)
	got := runTx(t, "MULTI", "DEL age name", "EXEC")
	expect(t, got, []string{ok, crossSlot, execAbort})
}

func TestTxKeylessAndKey(t *testing.T) {
	useTestLayout(t)
	got := runTx(t, "MULTI", "PING", "SET age 31", "EXEC", "GET age")
	expect(t, got, []string{ok, queued, queued,
		"*2\r\n+PONG\r\n+OK\r\n",
		"$2\r\n31\r\n"})
	commands.Dispatch(&commands.Command{Name: "DEL", Args: []string{"age"}})
}

func TestTxClusterOff(t *testing.T) {
	cluster.Current = nil
	got := runTx(t, "MULTI", "SET age 1", "SET name 2", "EXEC")
	expect(t, got, []string{ok, queued, queued, "*2\r\n+OK\r\n+OK\r\n"})
	commands.Dispatch(&commands.Command{Name: "DEL", Args: []string{"age", "name"}})
}
