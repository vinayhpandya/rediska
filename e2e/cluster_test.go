// Package e2e starts a real 3-node Rediska cluster (three separate
// processes) and checks it the way a client would: over TCP, with RESP.
//
// It is slower than the unit tests (it builds rediska first), so it is
// skipped by `go test -short ./...`.
package e2e

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"rediska/core"
)

// Slots used below (same as on your Mac):
//
//	age = 741 and foo2 = 1044  → node1
//	name = 5798, {user1} = 8106 → node2
//	visits = 13041              → node3
const (
	node1, node2, node3 = 0, 1, 2
)

// ---------------------------------------------------------------- cluster

type testCluster struct {
	addrs []string // "127.0.0.1:<port>" for node1..node3
}

// startCluster builds rediska, writes a cluster.conf with 3 nodes on free
// ports, starts one process per node and waits until each answers PING.
// Everything is stopped and deleted when the test ends, pass or fail.
func startCluster(t *testing.T) *testCluster {
	t.Helper()
	if testing.Short() {
		t.Skip("end-to-end test: skipped with -short")
	}
	dir := t.TempDir()

	// 1. Build. "rediska" is the module path, so this works from any folder.
	bin := filepath.Join(dir, "rediska")
	build := exec.Command("go", "build", "-o", bin, "rediska")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("go build failed: %v\n%s", err, out)
	}

	// 2. Pick 3 free ports and write the layout.
	c := &testCluster{}
	ports := freePorts(t, 3)
	ranges := []string{"0-5460", "5461-10922", "10923-16383"}
	var conf strings.Builder
	for i, p := range ports {
		addr := fmt.Sprintf("127.0.0.1:%d", p)
		c.addrs = append(c.addrs, addr)
		fmt.Fprintf(&conf, "node%d %s %s\n", i+1, addr, ranges[i])
	}
	confPath := filepath.Join(dir, "cluster.conf")
	if err := os.WriteFile(confPath, []byte(conf.String()), 0o644); err != nil {
		t.Fatal(err)
	}

	// 3. Start one process per node.
	for i, p := range ports {
		logf, err := os.Create(filepath.Join(dir, fmt.Sprintf("node%d.log", i+1)))
		if err != nil {
			t.Fatal(err)
		}
		cmd := exec.Command(bin, "--port", strconv.Itoa(p),
			"--cluster-enabled", "--cluster-config-file", confPath)
		cmd.Stdout, cmd.Stderr = logf, logf
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			cmd.Process.Signal(syscall.SIGTERM) // rediska shuts down cleanly on SIGTERM
			done := make(chan struct{})
			go func() { cmd.Wait(); close(done) }()
			select {
			case <-done:
			case <-time.After(3 * time.Second):
				cmd.Process.Kill()
			}
			logf.Close()
		})
	}

	// 4. Wait until every node answers PING.
	for i, addr := range c.addrs {
		deadline := time.Now().Add(5 * time.Second)
		for {
			conn, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
			if err == nil {
				conn.Close()
				break
			}
			if time.Now().After(deadline) {
				log, _ := os.ReadFile(filepath.Join(dir, fmt.Sprintf("node%d.log", i+1)))
				t.Fatalf("node%d (%s) did not start:\n%s", i+1, addr, log)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
	return c
}

// freePorts asks the OS for n unused ports, so the test never clashes with
// a cluster you already have running on 7001-7003.
func freePorts(t *testing.T, n int) []int {
	t.Helper()
	var ports []int
	var ls []net.Listener
	for range n {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		ls = append(ls, l)
		ports = append(ports, l.Addr().(*net.TCPAddr).Port)
	}
	for _, l := range ls {
		l.Close()
	}
	return ports
}

// ----------------------------------------------------------------- client

// client is a tiny RESP client: one TCP connection to one node.
type client struct {
	t    *testing.T
	conn net.Conn
	r    *bufio.Reader
}

func (c *testCluster) dial(t *testing.T, node int) *client {
	t.Helper()
	conn, err := net.Dial("tcp", c.addrs[node])
	if err != nil {
		t.Fatal(err)
	}
	conn.SetDeadline(time.Now().Add(5 * time.Second))
	t.Cleanup(func() { conn.Close() })
	return &client{t: t, conn: conn, r: bufio.NewReader(conn)}
}

// do sends one command and returns its reply as text (see readReply).
func (c *client) do(args ...string) string {
	c.t.Helper()
	c.send(args...)
	return c.read()
}

func (c *client) send(args ...string) {
	c.t.Helper()
	if _, err := c.conn.Write(core.EncodeArray(args)); err != nil {
		c.t.Fatal(err)
	}
}

func (c *client) read() string {
	c.t.Helper()
	s, err := readReply(c.r)
	if err != nil {
		c.t.Fatal(err)
	}
	return s
}

// readReply reads one RESP reply and turns it into text that keeps the reply
// TYPE, so a test can tell an error from a normal string:
//
//	+OK        → "OK"
//	-MOVED ... → "ERR:MOVED ..."
//	:5         → "5"
//	$5 vinay   → "vinay"
//	$-1        → "(nil)"
//	*2 ...     → "[a b]"
//
// (core.Decode is made for reading commands, and returns +OK and -ERR the
// same way, so the test can't use it here.)
func readReply(r *bufio.Reader) (string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return "", err
	}
	line = strings.TrimSuffix(line, "\r\n")
	if line == "" {
		return "", fmt.Errorf("empty reply line")
	}
	body := line[1:]
	switch line[0] {
	case '+', ':':
		return body, nil
	case '-':
		return "ERR:" + body, nil
	case '$':
		n, _ := strconv.Atoi(body)
		if n < 0 {
			return "(nil)", nil
		}
		buf := make([]byte, n+2)
		if _, err := io.ReadFull(r, buf); err != nil {
			return "", err
		}
		return string(buf[:n]), nil
	case '*':
		n, _ := strconv.Atoi(body)
		items := make([]string, 0, n)
		for range n {
			s, err := readReply(r)
			if err != nil {
				return "", err
			}
			items = append(items, s)
		}
		return "[" + strings.Join(items, " ") + "]", nil
	}
	return "", fmt.Errorf("unknown reply %q", line)
}

func expect(t *testing.T, what, got, want string) {
	t.Helper()
	if got != want {
		t.Errorf("%s:\n  got  %q\n  want %q", what, got, want)
	}
}

// ------------------------------------------------------------------ tests

// TestCluster runs every check against ONE cluster, so the build and
// startup cost is paid once. Each t.Run is a separate, named check.
func TestCluster(t *testing.T) {
	c := startCluster(t)
	moved := func(slot, node int) string {
		return fmt.Sprintf("ERR:MOVED %d %s", slot, c.addrs[node])
	}
	const crossSlot = "ERR:CROSSSLOT Keys in request don't hash to the same slot"
	const execAbort = "ERR:EXECABORT Transaction discarded because of previous errors."

	t.Run("MOVED points to the owner, and the owner accepts", func(t *testing.T) {
		expect(t, "SET name on node1", c.dial(t, node1).do("SET", "name", "vinay"), moved(5798, node2))
		n2 := c.dial(t, node2)
		expect(t, "SET name on node2", n2.do("SET", "name", "vinay"), "OK")
		expect(t, "GET name on node2", n2.do("GET", "name"), "vinay")
	})

	t.Run("keys live only on their owner", func(t *testing.T) {
		c.dial(t, node1).do("SET", "age", "30")
		c.dial(t, node3).do("SET", "visits", "1")
		for node, want := range []string{"keys=1", "keys=1", "keys=1"} {
			info := c.dial(t, node).do("INFO", "keyspace")
			if !strings.Contains(info, want) {
				t.Errorf("node%d: %q, want %s", node+1, info, want)
			}
		}
	})

	t.Run("CROSSSLOT and hash tags", func(t *testing.T) {
		n1 := c.dial(t, node1)
		expect(t, "DEL age name", n1.do("DEL", "age", "name"), crossSlot)
		// Same tag → same slot (8106, node2's): MOVED, not CROSSSLOT.
		expect(t, "DEL {user1}.* on node1", n1.do("DEL", "{user1}.a", "{user1}.b"), moved(8106, node2))
		n2 := c.dial(t, node2)
		n2.do("SET", "{user1}.a", "1")
		n2.do("SET", "{user1}.b", "2")
		expect(t, "DEL {user1}.* on node2", n2.do("DEL", "{user1}.a", "{user1}.b"), "2")
	})

	t.Run("keyless commands run anywhere", func(t *testing.T) {
		for node := range c.addrs {
			expect(t, fmt.Sprintf("PING node%d", node+1), c.dial(t, node).do("PING"), "PONG")
		}
	})

	t.Run("pipeline replies stay in order", func(t *testing.T) {
		n1 := c.dial(t, node1)
		// Send all three before reading anything: a real pipeline.
		n1.send("SET", "age", "40")
		n1.send("SET", "name", "x")
		n1.send("GET", "age")
		expect(t, "reply 1", n1.read(), "OK")
		expect(t, "reply 2", n1.read(), moved(5798, node2))
		expect(t, "reply 3", n1.read(), "40")
	})

	t.Run("MULTI with a key owned elsewhere aborts", func(t *testing.T) {
		n1 := c.dial(t, node1)
		n1.do("SET", "age", "50")
		expect(t, "MULTI", n1.do("MULTI"), "OK")
		expect(t, "SET age", n1.do("SET", "age", "99"), "QUEUED")
		expect(t, "SET name", n1.do("SET", "name", "y"), moved(5798, node2))
		expect(t, "EXEC", n1.do("EXEC"), execAbort)
		expect(t, "age unchanged", n1.do("GET", "age"), "50")
	})

	t.Run("MULTI across two of my slots is CROSSSLOT at EXEC", func(t *testing.T) {
		n1 := c.dial(t, node1)
		n1.do("SET", "age", "50")
		expect(t, "MULTI", n1.do("MULTI"), "OK")
		expect(t, "SET age", n1.do("SET", "age", "99"), "QUEUED")
		expect(t, "SET foo2", n1.do("SET", "foo2", "x"), "QUEUED")
		expect(t, "EXEC", n1.do("EXEC"), crossSlot)
		expect(t, "age unchanged", n1.do("GET", "age"), "50")
		expect(t, "foo2 not set", n1.do("GET", "foo2"), "(nil)")
	})

	t.Run("MULTI in one slot works", func(t *testing.T) {
		n1 := c.dial(t, node1)
		n1.do("MULTI")
		n1.do("SET", "age", "30")
		n1.do("INCR", "age")
		expect(t, "EXEC", n1.do("EXEC"), "[OK 31]")
	})

	t.Run("every node reports the same CLUSTER SLOTS", func(t *testing.T) {
		first := c.dial(t, node1).do("CLUSTER", "SLOTS")
		for _, part := range []string{"0 5460 [127.0.0.1", "5461 10922 [127.0.0.1", "10923 16383 [127.0.0.1"} {
			if !strings.Contains(first, part) {
				t.Errorf("CLUSTER SLOTS %q is missing %q", first, part)
			}
		}
		for node := range c.addrs {
			expect(t, fmt.Sprintf("node%d", node+1), c.dial(t, node).do("CLUSTER", "SLOTS"), first)
		}
	})

	t.Run("CLUSTER INFO and MYID", func(t *testing.T) {
		n1 := c.dial(t, node1)
		if info := n1.do("CLUSTER", "INFO"); !strings.Contains(info, "cluster_state:ok") {
			t.Errorf("CLUSTER INFO: %q", info)
		}
		ids := map[string]bool{}
		for node := range c.addrs {
			ids[c.dial(t, node).do("CLUSTER", "MYID")] = true
		}
		if len(ids) != 3 {
			t.Errorf("want 3 different node IDs, got %v", ids)
		}
	})
}
