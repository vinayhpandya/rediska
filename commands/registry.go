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
	"SET":    true,
	"EXPIRE": true,
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
func Dispatch(cmd *Command) []byte {
	handler, ok := commands[cmd.Name]
	if !ok {
		return core.EncodeError(fmt.Sprintf("ERR unknown command '%s'", cmd.Name))
	}
	reply := handler(cmd.Args)
	if writeCommands[cmd.Name] && config.MaxMemory > 0 {
		maxBytes := int64(config.MaxMemory) << 20
		switch config.MaxMemoryPolicy {
		case "allkeys-lru":
			store.Default.PerformEvictions(config.MaxMemorySamples, maxBytes)
		case "noeviction":
		}
		if store.Default.OverMemory(maxBytes) { // still over after the switch → reject
			return core.EncodeError("OOM command not allowed when used memory > 'maxmemory'.")
		}
	}
	if aof != nil && writeCommands[cmd.Name] {
		fullCmd := append([]string{cmd.Name}, cmd.Args...)
		if err := aof.Write(fullCmd); err != nil {
			fmt.Printf("AOF write error: %v\n", err)
		}
	}
	return reply

}
