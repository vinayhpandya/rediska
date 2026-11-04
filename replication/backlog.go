package replication

// A backlog keeps the master's most recent changes, already encoded, so a
// replica that was disconnected for a moment can be sent just the changes
// it missed instead of a full copy (see Master.Serve).
//
// Every change has an offset: 1 for the first change the master records,
// 2 for the next, and so on. A MULTI/EXEC block is one change. A replica
// remembers the offset of the last change it applied.
type backlog struct {
	items []backlogItem // oldest first
	bytes int           // total size of items' data
	max   int           // bytes to keep; the oldest changes are dropped past this
}

type backlogItem struct {
	offset int64
	data   []byte // the change, encoded as it's sent to replicas
}

func newBacklog(maxBytes int) *backlog {
	return &backlog{max: maxBytes}
}

// add records the change with this offset. Offsets must be added in order.
// The oldest changes are dropped until the backlog fits in its size again;
// the newest change is always kept, even if it alone is too big.
func (b *backlog) add(offset int64, data []byte) {
	b.items = append(b.items, backlogItem{offset, data})
	b.bytes += len(data)
	for b.bytes > b.max && len(b.items) > 1 {
		b.bytes -= len(b.items[0].data)
		b.items[0] = backlogItem{} // let the data be garbage-collected
		b.items = b.items[1:]
	}
}

// since returns every change after offset, joined in order, for a replica
// that has applied everything up to offset. latest is the master's newest
// offset. ok is false if the backlog can't bring that replica up to date:
// some of the changes it needs have been dropped, or it claims changes the
// master never made.
func (b *backlog) since(offset, latest int64) (data []byte, ok bool) {
	switch {
	case offset > latest:
		return nil, false // ahead of us: it can't be our history
	case offset == latest:
		return nil, true // nothing missed
	case len(b.items) == 0 || b.items[0].offset > offset+1:
		return nil, false // the first change it needs was dropped
	}
	for _, it := range b.items[offset+1-b.items[0].offset:] {
		data = append(data, it.data...)
	}
	return data, true
}
