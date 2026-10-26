package cluster

// The bus's goroutines never change peers themselves. When something
// happens (the timer fires, a dial finishes, a message arrives, a
// connection closes) they describe it as an Event and send it to
// dispatchWorker, which calls Handle. Every kind of event is listed here,
// in one switch.

// EventKind says what happened.
type EventKind int

const (
	EvTick       EventKind = iota // the 100 ms timer fired
	EvLinkUp                      // a dial succeeded: ID, Link
	EvDialFailed                  // a dial failed: ID
	EvMessage                     // a message arrived: Msg, Link
	EvLinkDown                    // a dialed connection closed: ID, Link
)

// Event is one thing that happened on the bus. Only the fields its Kind
// needs are set.
type Event struct {
	Kind EventKind
	ID   string  // which peer
	Link *link   // which connection
	Msg  Message // what arrived
}

// Handle runs on dispatchWorker, one event at a time, so it can read and
// change peers freely.
func (b *Bus) Handle(e Event) {
	switch e.Kind {
	case EvTick:
		b.tick()
	case EvLinkUp:
		b.linkUp(e.ID, e.Link)
	case EvDialFailed:
		b.dialFailed(e.ID)
	case EvMessage:
		b.handle(e.Msg, e.Link)
	case EvLinkDown:
		b.linkDown(e.ID, e.Link)
	}
}
