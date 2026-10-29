package cluster

import (
	"log"
	"time"
)

// Failure detection (Phase 4d). Everything here runs on dispatchWorker.
//
// It happens in two stages, as in Redis:
//
//   - PFAIL ("fail?"): one node's suspicion. A peer that hasn't answered us
//     for longer than NodeTimeout is PFAIL. Nobody else has to agree, and
//     it changes nothing for clients. We just mention it in our gossip.
//   - FAIL ("fail"): the group's decision. When we suspect a node ourselves
//     and enough other masters have reported it in their gossip that, with
//     us, they make a majority, we mark it FAIL and tell every node at once
//     with a FAIL message.
//
// Needing a majority means one node with a bad connection can't take
// another node out of the cluster, and the two halves of a split network
// can't both declare the other half dead.

// reportValidity: a failure report older than this many node timeouts no
// longer counts. The reporter repeats it in its gossip while it still
// suspects the node, so a report that stops arriving has gone stale.
const reportValidity = 2

// startWaiting starts the failure clock for p, if it isn't running yet.
// PingSent means "we've been waiting for p to answer since then". It's set
// when we send a PING, and also when we can't reach p at all (no link, so
// no PINGs go out), or a dead node would never look late.
func startWaiting(p *Peer, now time.Time) {
	if p.PingSent.IsZero() {
		p.PingSent = now
	}
}

// checkTimeout is called from tick for every peer: if p has been silent
// for longer than NodeTimeout, we start suspecting it.
//
// A peer still in handshake is skipped: it has never answered us, so there
// is nothing to compare with (and handshakeTimeout forgets it anyway).
func (b *Bus) checkTimeout(p *Peer, now time.Time) {
	if p.Handshake || p.flags&(FlagPFail|FlagFail) != 0 || p.PingSent.IsZero() {
		return
	}
	if now.Sub(p.PingSent) < b.NodeTimeout {
		return
	}
	p.flags |= FlagPFail
	log.Printf("cluster bus: %s hasn't answered for %v; marking it PFAIL",
		p.Node.Name, now.Sub(p.PingSent).Round(time.Millisecond))
	b.markFailedIfAgreed(p) // reports may already be waiting for us
}

// noteReport records what reporter's gossip says about p's health. If it
// says PFAIL or FAIL, that's a failure report from reporter; anything else
// takes back an earlier report.
//
// Like Redis, only masters' reports count. Every Rediska node is a master
// until replicas arrive, so for now that's every trusted node (learnFrom
// only calls us for those).
func (b *Bus) noteReport(p, reporter *Peer, flags NodeFlags) {
	if flags&(FlagPFail|FlagFail) == 0 {
		delete(p.failReports, reporter.Node.ID)
		return
	}
	if p.failReports == nil {
		p.failReports = map[string]time.Time{}
	}
	p.failReports[reporter.Node.ID] = time.Now()
	b.markFailedIfAgreed(p)
}

// markFailedIfAgreed marks p FAIL if we suspect it ourselves and, counting
// us, a majority of masters do. It's called from both places where the
// count can change: when we start suspecting p (checkTimeout), and when a
// report arrives (noteReport).
func (b *Bus) markFailedIfAgreed(p *Peer) {
	if !p.PFail() || p.Failed() {
		return // only our own suspicion can start it; FAIL is already final
	}
	needed := b.mastersWithSlots()/2 + 1
	agree := 1 + b.liveReports(p) // us, plus the masters that reported it
	if agree < needed {
		return
	}
	p.flags = FlagFail // replaces PFAIL
	log.Printf("cluster bus: %s marked FAIL (%d of %d needed masters agree)", p.Node.Name, agree, needed)
	b.broadcastFail(p)
}

// mastersWithSlots counts the nodes that own at least one slot, including
// us and including nodes that may have failed. A majority of these must
// agree before a node is marked FAIL. (Redis calls this the cluster size.)
func (b *Bus) mastersWithSlots() int {
	n := 0
	for _, node := range b.layout.Nodes {
		if len(node.Slots) > 0 {
			n++
		}
	}
	return n
}

// liveReports drops p's reports that are too old, and counts the rest.
func (b *Bus) liveReports(p *Peer) int {
	maxAge := reportValidity * b.NodeTimeout
	now := time.Now()
	for id, at := range p.failReports {
		if now.Sub(at) > maxAge || b.peers[id] == nil {
			delete(p.failReports, id) // stale, or from a node we've forgotten
		}
	}
	return len(p.failReports)
}

// broadcastFail tells every peer we're connected to that p has failed, so
// they mark it FAIL at once instead of waiting to count reports themselves.
func (b *Bus) broadcastFail(p *Peer) {
	for _, q := range b.peers {
		if q == p || q.link == nil || q.Handshake {
			continue
		}
		m := b.msg("FAIL", q.Node.ID)
		m.Gossip = []GossipEntry{gossipEntry(p)} // the failed node, and only it
		q.link.send(m)
	}
}

// failFrom handles a FAIL message: a trusted node says a majority agreed
// the node in its single gossip entry has failed. We accept that without
// counting ourselves. A FAIL about us, or about a node we don't know, is
// ignored.
func (b *Bus) failFrom(m Message) {
	sender := b.peers[m.ID]
	if sender == nil || sender.Handshake || len(m.Gossip) != 1 {
		return
	}
	p := b.peers[m.Gossip[0].ID]
	if p == nil || p.Failed() {
		return
	}
	p.flags = FlagFail
	log.Printf("cluster bus: %s marked FAIL (told by %s)", p.Node.Name, sender.Node.Name)
}

// clearFailure is called when p answers us: it's alive, so we stop
// suspecting it and clear FAIL too.
//
// Redis is stricter about clearing FAIL, because a replica may have taken
// over the failed master's slots by then. Rediska has no replicas yet, so
// "it answered" is always enough. This changes with failover.
func (b *Bus) clearFailure(p *Peer) {
	if p.flags&(FlagPFail|FlagFail) == 0 {
		return
	}
	if p.Failed() {
		log.Printf("cluster bus: %s is reachable again; clearing FAIL", p.Node.Name)
	} else {
		log.Printf("cluster bus: %s is answering again; clearing PFAIL", p.Node.Name)
	}
	p.flags &^= FlagPFail | FlagFail
	clear(p.failReports)
}
