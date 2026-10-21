package commands

import (
	"fmt"
	"rediska/config"
	"rediska/core"
	"rediska/store"
)

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

func SetAOF(a *core.AOF) {
	aof = a
}
func Register(name string, f HandlerFunc) {
	commands[name] = f
}

func init() {
	Register("_EXPIRY", func(args []string) []byte {
		deleted := store.Default.DeleteExpired()
		return core.EncodeInteger(int64(deleted))
	})
}
func IsKnownCommand(name string) bool {
	_, ok := commands[name]
	return ok
}

func isErrorReply(reply []byte) bool {
	return len(reply) > 0 && reply[0] == '-'
}
func Dispatch(cmd *Command) []byte {
	handler, ok := commands[cmd.Name]
	if !ok {
		return core.EncodeError(fmt.Sprintf("ERR unknown command '%s'", cmd.Name))
	}
	if errReply := checkCluster(cmd); errReply != nil { // ← new
		return errReply
	}
	reply := handler(cmd.Args)
	if denyOOMCommands[cmd.Name] && config.MaxMemory > 0 {
		maxBytes := int64(config.MaxMemory) << 20
		switch config.MaxMemoryPolicy {
		case "allkeys-lru", "allkeys-lfu", "volatile-lfu":
			store.Default.PerformEvictions(config.MaxMemorySamples, maxBytes)
		case "noeviction":
		}
		if store.Default.OverMemory(maxBytes) { // still over after the switch → reject
			return core.EncodeError("OOM command not allowed when used memory > 'maxmemory'.")
		}
	}
	if aof != nil && writeCommands[cmd.Name] && !isErrorReply(reply) {
		fullCmd := append([]string{cmd.Name}, cmd.Args...)
		if err := aof.Write(fullCmd); err != nil {
			fmt.Printf("AOF write error: %v\n", err)
		}
	}
	return reply

}

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
