// Package commands implements every Redis command Rediska supports.
//
// Each command lives in its own file and registers a handler with Register
// from an init function. Dispatch looks the handler up and runs it; in
// cluster mode it first checks that this node owns the command's keys.
//
// All functions that touch data (Dispatch, DispatchBatch, DispatchPipeline)
// must run on the server's single dispatch worker goroutine, because the
// store is not safe for concurrent use.
package commands

import (
	"fmt"

	"github.com/vinayhpandya/rediska/cluster"
	"github.com/vinayhpandya/rediska/config"
	"github.com/vinayhpandya/rediska/core"
	"github.com/vinayhpandya/rediska/replication"
	"github.com/vinayhpandya/rediska/store"
)

// HandlerFunc runs one command. args are the command's arguments without
// the command name, and the return value is the full encoded RESP reply.
type HandlerFunc func(args []string) []byte

var commands = make(map[string]HandlerFunc)

var aof *core.AOF

var writeCommands = map[string]bool{
	"SET":       true,
	"EXPIRE":    true,
	"PEXPIREAT": true,
	"INCR":      true,
	"APPEND":    true,
	"PFADD":     true,
	"PFMERGE":   true,

	"_HLLRESTORE": true,
}

var denyOOMCommands = map[string]bool{
	"SET":     true,
	"INCR":    true,
	"APPEND":  true,
	"PFADD":   true,
	"PFMERGE": true,
}

// SetAOF makes Dispatch append successful write commands to a. Call it
// only after the AOF has been replayed at startup; otherwise every
// replayed command would be written to the file a second time.
func SetAOF(a *core.AOF) {
	aof = a
	updateTracking()
}

func init() {
	replication.Default.OnChange = updateTracking
}

// recording reports whether anyone needs to hear about changes: the AOF,
// or replication. Once a replica has connected, replication records every
// change, even while no replica is connected (see Master.Active).
func recording() bool {
	return aof != nil || replication.Default.Active()
}

// updateTracking turns deletion tracking on while changes are recorded,
// so that keys removed by expiry or eviction reach the AOF and replicas
// too. It's called when the AOF is set and when replicas come and go.
func updateTracking() {
	store.Default.TrackDeletes(recording())
}

// While DispatchBatch runs a MULTI/EXEC, its entries are collected in
// txEntries instead of being written one at a time, and written together
// at the end (see writeEntry and DispatchBatch).
var (
	inTx      bool
	txEntries [][]string
)

// logChange records what cmd changed, in the order it happened, for the
// AOF and for replicas:
//
//  1. a DEL for every key removed while it ran, whatever removed it: DEL
//     itself, lazy expiry in a read, active expiry (_EXPIRY), or eviction
//     before a write. Without these, a restart brings the keys back.
//  2. the command itself, if it's a write that succeeded, rewritten by
//     aofForm so that replaying it later has the same effect.
func logChange(cmd *Command, reply []byte) {
	for _, key := range store.Default.TakeDeleted() {
		writeEntry([]string{"DEL", key})
	}
	if !writeCommands[cmd.Name] || isErrorReply(reply) {
		return
	}
	if args := aofForm(cmd); args != nil {
		writeEntry(args)
	}
}

// writeEntry writes one entry to the AOF and sends it to replicas, or,
// inside a MULTI/EXEC, keeps it for DispatchBatch to handle with the rest
// of the transaction.
func writeEntry(args []string) {
	if inTx {
		txEntries = append(txEntries, args)
		return
	}
	if aof != nil {
		if err := aof.Write(args); err != nil {
			fmt.Printf("AOF write error: %v\n", err)
		}
	}
	if replication.Default.Active() {
		replication.Default.Feed(replication.Entry{Slot: entrySlot(args), Args: args})
	}
}

// entrySlot is the slot of an entry's keys. Every entry has keys, and in
// cluster mode they're all in one slot, so the first key decides.
func entrySlot(args []string) int {
	keys, _ := KeysOf(&Command{Name: args[0], Args: args[1:]})
	if len(keys) == 0 {
		return -1 // not expected; sent to every replica
	}
	return cluster.KeyHashSlot(keys[0])
}

// aofForm returns what the AOF and replicas should get for a successful
// write.
// Expiry is recorded as an absolute time, because a TTL means "from now",
// and "now" is a different time when the file is replayed:
//
//	SET k v EX 10  →  SET k v PXAT <unix ms>
//	EXPIRE k 60    →  PEXPIREAT k <unix ms>
//
// It returns nil if the command removed the key instead (an expiry time
// already in the past): its DEL has been logged already.
func aofForm(cmd *Command) []string {
	switch {
	case cmd.Name == "SET" && len(cmd.Args) == 4,
		cmd.Name == "EXPIRE" && len(cmd.Args) == 2:
		key := cmd.Args[0]
		at, ok := store.Default.ExpiresAt(key)
		if !ok {
			return nil
		}
		ms := fmt.Sprint(at.UnixMilli())
		if cmd.Name == "SET" {
			return []string{"SET", key, cmd.Args[1], "PXAT", ms}
		}
		return []string{"PEXPIREAT", key, ms}
	}
	return append([]string{cmd.Name}, cmd.Args...)
}

// Register adds a command. name must be upper case, since Dispatch looks
// commands up by their upper-cased name. Registering a name twice replaces
// the earlier handler. Command files call Register from their init function.
func Register(name string, f HandlerFunc) {
	commands[name] = f
}

func init() {
	Register("_EXPIRY", func(args []string) []byte {
		deleted := store.Default.DeleteExpired()
		return core.EncodeInteger(int64(deleted))
	})
}

// IsKnownCommand reports whether a command called name (upper case) has
// been registered. MULTI uses it to reject unknown commands when they are
// queued, rather than at EXEC.
func IsKnownCommand(name string) bool {
	_, ok := commands[name]
	return ok
}

func isErrorReply(reply []byte) bool {
	return len(reply) > 0 && reply[0] == '-'
}

// Dispatch must only be called from the dispatch worker goroutine.
func Dispatch(cmd *Command) []byte {
	reply := run(cmd)
	if recording() {
		// Even when cmd was refused (OOM), eviction may have removed keys
		// first; logChange records their DELs.
		logChange(cmd, reply)
	}
	return reply
}

// run is Dispatch without the AOF: the checks, then the handler.
func run(cmd *Command) []byte {
	handler, ok := commands[cmd.Name]
	if !ok {
		return core.EncodeError(fmt.Sprintf("ERR unknown command '%s'", cmd.Name))
	}
	if errReply := checkCluster(cmd); errReply != nil {
		return errReply // CROSSSLOT, CLUSTERDOWN or MOVED: nothing was run
	}
	// Check memory BEFORE running the command, as Redis does, so a write
	// that's rejected for OOM has no effect.
	if denyOOMCommands[cmd.Name] && config.MaxMemory > 0 {
		maxBytes := int64(config.MaxMemory) << 20
		switch config.MaxMemoryPolicy {
		case "allkeys-lru", "allkeys-lfu", "volatile-lfu":
			store.Default.PerformEvictions(config.MaxMemorySamples, maxBytes)
		case "noeviction":
		}
		if store.Default.OverMemory(maxBytes) { // still over after evicting → reject
			return core.EncodeError("OOM command not allowed when used memory > 'maxmemory'.")
		}
	}
	return handler(cmd.Args)
}

// Replay runs a command read back from the AOF at startup. Unlike
// Dispatch, it skips the cluster check and the memory check, and doesn't
// write to the AOF: everything in this node's AOF was already accepted by
// this node, so it must all be loaded back, even if the cluster layout has
// changed since. Unknown commands are ignored.
func Replay(cmd *Command) {
	if handler, ok := commands[cmd.Name]; ok {
		handler(cmd.Args)
	}
}

// DispatchBatch runs the commands queued by MULTI when the client sends
// EXEC, and returns their replies wrapped in one RESP array.
//
// In cluster mode, all keys of all the commands together must be in one
// slot owned by this node. If they aren't, DispatchBatch returns that
// single error (CROSSSLOT, CLUSTERDOWN or MOVED) and runs nothing, so a
// transaction never runs halfway.
//
// With an AOF, the transaction's entries are written as one block,
// wrapped in MULTI and EXEC, so the file has all of it or none of it.
func DispatchBatch(cmd []*Command) []byte {
	if errReply := checkClusterBatch(cmd); errReply != nil {
		return errReply // the whole transaction is refused; nothing ran
	}
	inTx, txEntries = true, nil
	replies := make([][]byte, len(cmd))
	for i, c := range cmd {
		replies[i] = Dispatch(c)
	}
	inTx = false
	// The whole transaction is written and sent at once, so the AOF and
	// replicas get all of it or none of it. One that changed nothing
	// writes nothing.
	if len(txEntries) > 0 {
		if aof != nil {
			if err := aof.WriteTransaction(txEntries); err != nil {
				fmt.Printf("AOF write error: %v\n", err)
			}
		}
		if replication.Default.Active() {
			entries := make([]replication.Entry, len(txEntries))
			for i, args := range txEntries {
				entries[i] = replication.Entry{Slot: entrySlot(args), Args: args}
			}
			replication.Default.FeedTransaction(entries)
		}
	}
	txEntries = nil
	return core.EncodeRawArray(replies)
}

// DispatchPipeline runs each command in order and returns their replies
// concatenated back-to-back, exactly as if each had been dispatched and
// written individually.
func DispatchPipeline(cmds []*Command) []byte {
	var out []byte
	for _, c := range cmds {
		out = append(out, Dispatch(c)...)
	}
	return out
}
