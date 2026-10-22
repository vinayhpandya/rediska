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
	"github.com/vinayhpandya/rediska/config"
	"github.com/vinayhpandya/rediska/core"
	"github.com/vinayhpandya/rediska/store"
)

// HandlerFunc runs one command. args are the command's arguments without
// the command name, and the return value is the full encoded RESP reply.
type HandlerFunc func(args []string) []byte

var commands = make(map[string]HandlerFunc)

var aof *core.AOF

var writeCommands = map[string]bool{
	"SET":     true,
	"EXPIRE":  true,
	"INCR":    true,
	"APPEND":  true,
	"PFADD":   true,
	"PFMERGE": true,
	"DEL":     true,
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

// Dispatch runs one command and returns its encoded RESP reply.
//
// It replies with an error, without running anything, if the command is
// unknown or, in cluster mode, if its keys span several slots (CROSSSLOT),
// belong to no node (CLUSTERDOWN) or belong to another node (MOVED).
//
// For commands that can grow memory, and only when --maxmemory is set, it
// then evicts keys according to the policy, and replies with an OOM error
// without running the command if memory is still over the limit.
//
// Otherwise it runs the handler, and appends successful write commands to
// the AOF if one is set (see SetAOF).
//
// Dispatch must only be called from the dispatch worker goroutine.
func Dispatch(cmd *Command) []byte {
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
	reply := handler(cmd.Args)
	if aof != nil && writeCommands[cmd.Name] && !isErrorReply(reply) {
		fullCmd := append([]string{cmd.Name}, cmd.Args...)
		if err := aof.Write(fullCmd); err != nil {
			fmt.Printf("AOF write error: %v\n", err)
		}
	}
	return reply

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
func DispatchBatch(cmd []*Command) []byte {
	if errReply := checkClusterBatch(cmd); errReply != nil {
		return errReply // the whole transaction is refused; nothing ran
	}
	replies := make([][]byte, len(cmd))
	for i, c := range cmd {
		replies[i] = Dispatch(c)
	}
	return core.EncodeRawArray(replies)
}

// DispatchPipeline runs each command in order and returns their replies
// concatenated back-to-back -- exactly as if each had been dispatched and
// written individually, one at a time. Unlike DispatchBatch (used for
// MULTI/EXEC, which needs one array-wrapped reply representing the whole
// transaction), pipelined commands each need their own independent,
// individually-parseable reply, so no outer array framing is added here.
func DispatchPipeline(cmds []*Command) []byte {
	var out []byte
	for _, c := range cmds {
		out = append(out, Dispatch(c)...)
	}
	return out
}
