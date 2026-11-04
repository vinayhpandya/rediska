package replication

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/vinayhpandya/rediska/core"
)

// RetryInterval is how long a replica waits before connecting to its
// master again, after it couldn't connect or the connection dropped.
var RetryInterval = time.Second // a variable so tests can shorten it

// Link is a replica's connection to its master.
type Link struct {
	MasterAddr string
	up         atomic.Bool

	// What this replica has: everything up to change offset of the
	// master's history replID. offset is -1, and replID "", while it has
	// nothing it can resume from: before its first copy has finished.
	// Only Run's goroutine changes them; offset is atomic for INFO.
	replID string
	offset atomic.Int64

	mu   sync.Mutex
	conn net.Conn // the current connection, for Reconnect; nil between them
}

// Up reports whether the replica is connected to its master and receiving
// data. INFO replication shows it as master_link_status. Safe to call
// from any goroutine.
func (l *Link) Up() bool { return l != nil && l.up.Load() }

// Offset is the offset of the last change this replica applied, or -1 if
// it has nothing it can resume from yet. INFO replication shows it as
// slave_repl_offset. Safe to call from any goroutine.
func (l *Link) Offset() int64 { return l.offset.Load() }

// Reconnect drops the connection to the master, as if the network had
// failed. The replica connects again after RetryInterval, and asks for
// just the changes it missed. DEBUG RECONNECT uses it, for testing. Safe
// to call from any goroutine.
func (l *Link) Reconnect() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.conn != nil {
		l.conn.Close()
	}
}

// Upstream is this node's link to its master, or nil if it's a master.
// The server sets it at startup, before any goroutine reads it.
var Upstream *Link

// NewLink returns a link to the master at masterAddr, not connected yet.
func NewLink(masterAddr string) *Link {
	l := &Link{MasterAddr: masterAddr}
	l.offset.Store(-1)
	return l
}

// Run keeps this node a copy of the master until ctx is cancelled. Each
// time it connects, it sends PSYNC with what it already has. If the master
// answers +CONTINUE, the replica keeps its data and applies the changes
// that follow. If it answers +FULLRESYNC, flush must clear this node's
// data first. apply must apply commands in order: one at a time, or a
// whole MULTI/EXEC block at once. flush and apply hand their work to the
// dispatch worker; they return false if the server is shutting down.
func (l *Link) Run(ctx context.Context, flush func() bool, apply func([][]string) bool) {
	masterAddr := l.MasterAddr
	for ctx.Err() == nil {
		err := l.syncOnce(ctx, flush, apply)
		l.up.Store(false)
		if ctx.Err() != nil {
			return
		}
		log.Printf("replication: link to master %s: %v; retrying", masterAddr, err)
		select {
		case <-time.After(RetryInterval):
		case <-ctx.Done():
		}
	}
}

func (l *Link) syncOnce(ctx context.Context, flush func() bool, apply func([][]string) bool) error {
	conn, err := net.DialTimeout("tcp", l.MasterAddr, time.Second)
	if err != nil {
		return err
	}
	defer conn.Close()
	stop := context.AfterFunc(ctx, func() { conn.Close() }) // unblocks the reads below
	defer stop()
	l.mu.Lock()
	l.conn = conn
	l.mu.Unlock()
	defer func() {
		l.mu.Lock()
		l.conn = nil
		l.mu.Unlock()
	}()

	replID, offset := "?", l.offset.Load()
	if l.replID != "" && offset >= 0 {
		replID = l.replID
	} else {
		offset = -1
	}
	if _, err := conn.Write(core.EncodeArray([]string{"PSYNC", replID, strconv.FormatInt(offset, 10)})); err != nil {
		return err
	}
	r := bufio.NewReader(conn)
	first, err := core.Decode(r)
	if err != nil {
		return err
	}
	switch first {
	case "CONTINUE":
		log.Printf("replication: continuing from master %s at change %d", l.MasterAddr, offset)
	case "FULLRESYNC":
		// Until the copy finishes, our data matches no offset, so a
		// disconnect from here on means a full copy again.
		l.replID = ""
		l.offset.Store(-1)
		if !flush() {
			return ctx.Err()
		}
		log.Printf("replication: copying everything from master %s", l.MasterAddr)
	default:
		return fmt.Errorf("master refused PSYNC: %v", first)
	}
	l.up.Store(true)

	var block [][]string // a MULTI/EXEC block being read, nil outside one
	inBlock := false
	for {
		v, err := core.Decode(r)
		if err != nil {
			return err
		}
		args, err := toArgs(v)
		if err != nil {
			return err
		}
		switch {
		case len(args) == 3 && args[0] == "_SYNCED":
			// The copy is finished: we have everything up to this offset.
			n, err := strconv.ParseInt(args[2], 10, 64)
			if err != nil {
				return fmt.Errorf("master sent a bad offset: %v", args)
			}
			l.replID = args[1]
			l.offset.Store(n)
			continue
		case len(args) == 1 && strings.EqualFold(args[0], "MULTI"):
			inBlock, block = true, nil
			continue
		case len(args) == 1 && strings.EqualFold(args[0], "EXEC") && inBlock:
			inBlock = false
			if len(block) > 0 && !apply(block) {
				return ctx.Err()
			}
			block = nil
		case inBlock:
			block = append(block, args)
			continue
		default:
			if !apply([][]string{args}) {
				return ctx.Err()
			}
		}
		// One change applied: a single command, or a whole MULTI/EXEC.
		if n := l.offset.Load(); n >= 0 {
			l.offset.Store(n + 1)
		}
	}
}

// toArgs turns one decoded RESP array of bulk strings into a command.
func toArgs(v any) ([]string, error) {
	arr, ok := v.([]any)
	if !ok || len(arr) == 0 {
		return nil, fmt.Errorf("master sent %v, not a command", v)
	}
	args := make([]string, len(arr))
	for i, x := range arr {
		s, ok := x.(string)
		if !ok {
			return nil, fmt.Errorf("master sent a command with a non-string argument: %v", v)
		}
		args[i] = s
	}
	return args, nil
}
