package replication

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"net"
	"strings"
	"sync/atomic"
	"time"

	"github.com/vinayhpandya/rediska/core"
)

// retryInterval is how long a replica waits before connecting to its
// master again, after it couldn't connect or the connection dropped.
var retryInterval = time.Second

// Link is a replica's connection to its master.
type Link struct {
	MasterAddr string
	up         atomic.Bool
}

// Up reports whether the replica is connected to its master and receiving
// data. INFO replication shows it as master_link_status. Safe to call
// from any goroutine.
func (l *Link) Up() bool { return l != nil && l.up.Load() }

// Upstream is this node's link to its master, or nil if it's a master.
// The server sets it at startup, before any goroutine reads it.
var Upstream *Link

// NewLink returns a link to the master at masterAddr, not connected yet.
func NewLink(masterAddr string) *Link { return &Link{MasterAddr: masterAddr} }

// Run keeps this node a copy of the master until ctx is cancelled. Each
// time it connects, it sends SYNC, then flush must clear this node's data,
// and apply must apply the commands that follow, in order: one command at
// a time, or a whole MULTI/EXEC block at once. flush and apply hand their
// work to the dispatch worker; they return false if the server is shutting
// down.
//
// Every connection starts from an empty store and a full copy, so if the
// connection drops, the replica simply connects again.
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
		case <-time.After(retryInterval):
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

	if _, err := conn.Write(core.EncodeArray([]string{"SYNC"})); err != nil {
		return err
	}
	r := bufio.NewReader(conn)
	first, err := core.Decode(r)
	if err != nil {
		return err
	}
	if s, ok := first.(string); !ok || s != "OK" {
		return fmt.Errorf("master refused SYNC: %v", first)
	}
	if !flush() {
		return ctx.Err()
	}
	l.up.Store(true)
	log.Printf("replication: syncing from master %s", l.MasterAddr)

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
		case len(args) == 1 && strings.EqualFold(args[0], "MULTI"):
			inBlock, block = true, nil
			continue
		case len(args) == 1 && strings.EqualFold(args[0], "EXEC") && inBlock:
			inBlock = false
			if len(block) > 0 && !apply(block) {
				return ctx.Err()
			}
			block = nil
			continue
		case inBlock:
			block = append(block, args)
			continue
		}
		if !apply([][]string{args}) {
			return ctx.Err()
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
