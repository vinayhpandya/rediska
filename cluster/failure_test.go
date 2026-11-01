package cluster

import (
	"testing"
	"time"
)

// Failure detection tests (Phase 4d). No network: like gossip_test.go, they
// use gossipFixture (we are node1; node2 and node3 are trusted peers on test
// links) and call the bus's functions the way dispatchWorker would.
//
// There are 3 masters with slots, so FAIL needs 2 to agree: us, plus one
// report. The clock is passed in to checkTimeout, so no test has to sleep.

const testTimeout = time.Second

func newFailureFixture(t *testing.T) *gossipFixture {
	f := newGossipFixture(t)
	f.b.NodeTimeout = testTimeout
	return f
}

// suspect makes us PFAIL p: it has had a PING unanswered for longer than
// the node timeout.
func (f *gossipFixture) suspect(p *Peer) {
	f.t.Helper()
	now := time.Now()
	p.PingSent = now.Add(-testTimeout - time.Millisecond)
	f.b.checkTimeout(p, now)
	if !p.PFail() && !p.Failed() { // straight to FAIL if reports were waiting
		f.t.Fatalf("%s: not suspected after the timeout", p.Node.Name)
	}
}

// report delivers a PING from reporter whose gossip says about's health.
func (f *gossipFixture) report(reporter, about string, flags NodeFlags) {
	n := f.node(about)
	f.b.handle(f.from("PING", reporter, f.node(reporter).Slots,
		GossipEntry{ID: n.ID, Name: n.Name, Addr: n.Addr, Flags: flags}), f.inbox)
	<-f.inbox.out // throw away our PONG
}

func wantHealth(t *testing.T, p *Peer, pfail, fail bool) {
	t.Helper()
	if p.PFail() != pfail || p.Failed() != fail {
		t.Fatalf("%s: PFail=%v Failed=%v, want PFail=%v Failed=%v",
			p.Node.Name, p.PFail(), p.Failed(), pfail, fail)
	}
}

// ================================================================== PFAIL

func TestPFailOnlyAfterTimeout(t *testing.T) {
	f := newFailureFixture(t)
	p := f.b.Peer(f.node("node3").ID)
	start := time.Now()
	p.PingSent = start

	f.b.checkTimeout(p, start.Add(testTimeout-time.Millisecond))
	wantHealth(t, p, false, false) // just under the timeout: still fine

	f.b.checkTimeout(p, start.Add(testTimeout))
	wantHealth(t, p, true, false) // our suspicion alone is PFAIL, never FAIL
}

func TestNoPFailWithoutAPendingPing(t *testing.T) {
	f := newFailureFixture(t)
	p := f.b.Peer(f.node("node3").ID)
	// PingSent is zero: we aren't waiting for p, so however late it is,
	// it can't be late.
	f.b.checkTimeout(p, time.Now().Add(time.Hour))
	wantHealth(t, p, false, false)
}

func TestHandshakePeerIsNeverSuspected(t *testing.T) {
	f := newFailureFixture(t)
	p := f.b.Peer(f.node("node3").ID)
	p.Handshake = true
	p.PingSent = time.Now()
	f.b.checkTimeout(p, time.Now().Add(time.Hour))
	wantHealth(t, p, false, false)
}

// A node we can't connect to sends no PONGs, but also gets no PINGs, so
// the clock has to start when the connection is lost or a dial fails.
func TestUnreachablePeerStartsTheClock(t *testing.T) {
	for _, how := range []string{"link down", "dial failed"} {
		t.Run(how, func(t *testing.T) {
			f := newFailureFixture(t)
			p := f.b.Peer(f.node("node3").ID)
			before := time.Now()
			if how == "link down" {
				f.b.linkDown(p.Node.ID, p.link)
				if p.Connected() {
					t.Fatal("still connected after linkDown")
				}
			} else {
				p.link, p.connecting = nil, true
				f.b.dialFailed(p.Node.ID)
			}
			if p.PingSent.Before(before) {
				t.Fatalf("clock not started: PingSent = %v", p.PingSent)
			}
			f.b.checkTimeout(p, p.PingSent.Add(testTimeout))
			wantHealth(t, p, true, false)
		})
	}
}

// ================================================================== FAIL

func TestFailNeedsAMajority(t *testing.T) {
	f := newFailureFixture(t)
	node2, node3 := f.b.Peer(f.node("node2").ID), f.b.Peer(f.node("node3").ID)

	f.suspect(node3)
	wantHealth(t, node3, true, false) // 1 of 2 needed

	f.report("node2", "node3", FlagPFail)
	wantHealth(t, node3, false, true) // 2 of 2: FAIL replaces PFAIL

	// We tell everyone else at once: node2 gets a FAIL naming node3.
	m := mustDecodeOut(t, node2.link)
	if m.Type != "FAIL" || len(m.Gossip) != 1 || m.Gossip[0].ID != node3.Node.ID {
		t.Fatalf("broadcast = %+v, want FAIL about node3", m)
	}
	// node3 itself isn't sent one.
	if len(node3.link.out) != 0 {
		t.Fatal("the failed node was sent the FAIL broadcast")
	}
}

// Reports alone never make a FAIL: we must suspect the node ourselves.
// But they're kept, so FAIL comes the moment we do.
func TestReportsWaitForOurOwnSuspicion(t *testing.T) {
	f := newFailureFixture(t)
	node3 := f.b.Peer(f.node("node3").ID)

	f.report("node2", "node3", FlagPFail)
	wantHealth(t, node3, false, false)

	f.suspect(node3) // checkTimeout counts the waiting report
	wantHealth(t, node3, false, true)
}

// A FAIL flag in gossip counts as a report too, the same as PFAIL.
func TestFailInGossipCountsAsAReport(t *testing.T) {
	f := newFailureFixture(t)
	node3 := f.b.Peer(f.node("node3").ID)
	f.report("node2", "node3", FlagFail)
	f.suspect(node3)
	wantHealth(t, node3, false, true)
}

// Gossip that says the node is fine takes back an earlier report.
func TestHealthyGossipWithdrawsAReport(t *testing.T) {
	f := newFailureFixture(t)
	node3 := f.b.Peer(f.node("node3").ID)
	f.report("node2", "node3", FlagPFail)
	f.report("node2", "node3", 0)
	f.suspect(node3)
	wantHealth(t, node3, true, false)
}

// A report older than reportValidity node timeouts no longer counts.
func TestStaleReportsDontCount(t *testing.T) {
	f := newFailureFixture(t)
	node2, node3 := f.node("node2"), f.b.Peer(f.node("node3").ID)
	node3.failReports = map[string]time.Time{
		node2.ID: time.Now().Add(-reportValidity*testTimeout - time.Second),
	}
	f.suspect(node3)
	wantHealth(t, node3, true, false)
	if len(node3.failReports) != 0 {
		t.Fatalf("stale report kept: %v", node3.failReports)
	}
}

// Reports from nodes we don't trust are ignored: a stranger or a peer still
// in handshake can't help take a node down.
func TestUntrustedReportsAreIgnored(t *testing.T) {
	f := newFailureFixture(t)
	node2, node3 := f.b.Peer(f.node("node2").ID), f.b.Peer(f.node("node3").ID)
	n3 := node3.Node
	gossip := GossipEntry{ID: n3.ID, Name: n3.Name, Addr: n3.Addr, Flags: FlagPFail}

	f.b.handle(Message{Type: "PING", ID: "stranger", Name: "x", Addr: "127.0.0.1:7999",
		Gossip: []GossipEntry{gossip}}, f.inbox)
	node2.Handshake = true
	f.report("node2", "node3", FlagPFail)

	f.suspect(node3)
	wantHealth(t, node3, true, false)
}

// With 5 masters, FAIL needs 3: us plus two reports.
func TestMajorityGrowsWithTheCluster(t *testing.T) {
	f := newFailureFixture(t)
	for i, n := range []*Node{
		{ID: "id-4", Name: "node4", Addr: "127.0.0.1:7004", Port: 7004},
		{ID: "id-5", Name: "node5", Addr: "127.0.0.1:7005", Port: 7005},
	} {
		p := f.b.addPeer(n, false)
		p.link = testLink()
		f.b.applySlots(p, []SlotRange{r(1000+i, 1000+i)}) // owns a slot, so it's counted
	}
	if got := f.b.mastersWithSlots(); got != 5 {
		t.Fatalf("mastersWithSlots = %d, want 5", got)
	}
	node3 := f.b.Peer(f.node("node3").ID)
	f.suspect(node3)
	f.report("node2", "node3", FlagPFail)
	wantHealth(t, node3, true, false) // 2 of 3

	f.b.handle(Message{Type: "PING", ID: "id-4", Name: "node4", Addr: "127.0.0.1:7004",
		Slots:  []SlotRange{r(1000, 1000)},
		Gossip: []GossipEntry{{ID: node3.Node.ID, Name: "node3", Addr: node3.Node.Addr, Flags: FlagPFail}}},
		f.inbox)
	wantHealth(t, node3, false, true) // 3 of 3
}

// ========================================================= FAIL messages

func TestFailMessageIsBelievedAtOnce(t *testing.T) {
	f := newFailureFixture(t)
	node3 := f.node("node3")
	m := f.from("FAIL", "node2", f.node("node2").Slots,
		GossipEntry{ID: node3.ID, Name: node3.Name, Addr: node3.Addr})
	f.b.handle(m, f.inbox)
	// We never suspected node3, but a trusted node says a majority agreed.
	wantHealth(t, f.b.Peer(node3.ID), false, true)
}

func TestFailMessageIgnoredFromUntrustedSenders(t *testing.T) {
	f := newFailureFixture(t)
	node3 := f.node("node3")
	about := GossipEntry{ID: node3.ID, Name: node3.Name, Addr: node3.Addr}

	f.b.handle(Message{Type: "FAIL", ID: "stranger", Name: "x", Addr: "127.0.0.1:7999",
		Gossip: []GossipEntry{about}}, f.inbox)
	wantHealth(t, f.b.Peer(node3.ID), false, false)

	f.b.Peer(f.node("node2").ID).Handshake = true
	f.b.handle(f.from("FAIL", "node2", nil, about), f.inbox)
	wantHealth(t, f.b.Peer(node3.ID), false, false)
}

func TestFailMessageAboutUsIsIgnored(t *testing.T) {
	f := newFailureFixture(t)
	self := f.l.Self
	f.b.handle(f.from("FAIL", "node2", f.node("node2").Slots,
		GossipEntry{ID: self.ID, Name: self.Name, Addr: self.Addr}), f.inbox)
	for _, p := range f.b.Peers() {
		wantHealth(t, p, false, false)
	}
}

// ============================================================ recovering

// A PONG on the node's own link means it's alive: PFAIL, FAIL and the
// reports about it are all cleared.
func TestPongClearsFailure(t *testing.T) {
	f := newFailureFixture(t)
	node3 := f.b.Peer(f.node("node3").ID)
	f.suspect(node3)
	f.report("node2", "node3", FlagPFail)
	wantHealth(t, node3, false, true)

	// A PONG claiming to be node3 on another connection proves nothing.
	pong := f.from("PONG", "node3", node3.Node.Slots)
	f.b.handle(pong, f.inbox)
	wantHealth(t, node3, false, true)

	f.b.handle(pong, node3.link)
	wantHealth(t, node3, false, false)
	if len(node3.failReports) != 0 || !node3.PingSent.IsZero() {
		t.Fatalf("after PONG: reports %v, PingSent %v; want none, zero",
			node3.failReports, node3.PingSent)
	}

	// The clock starts over: one old report doesn't linger to cause a FAIL.
	f.suspect(node3)
	wantHealth(t, node3, true, false)
}
