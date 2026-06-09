package commands

import (
	"fmt"
	"rediska/core"
)

type HandlerFunc func(args []string) []byte

var commands = make(map[string]HandlerFunc)

var aof *core.AOF

var writeCommands = map[string]bool{
	"SET": true,
}

func SetAOF(a *core.AOF) {
	aof = a
}
func Register(name string, f HandlerFunc) {
	commands[name] = f
}

func Dispatch(cmd *Command) []byte {
	handler, ok := commands[cmd.Name]
	if !ok {
		return core.EncodeError(fmt.Sprintf("ERR unknown command '%s'", cmd.Name))
	}
	reply := handler(cmd.Args)

	if aof != nil && writeCommands[cmd.Name] {
		fullCmd := append([]string{cmd.Name}, cmd.Args...)
		if err := aof.Write(fullCmd); err != nil {
			fmt.Printf("AOF write error: %v\n", err)
		}
	}
	return reply

}
