package commands

import (
	"fmt"
	"rediska/core"
)

type HandlerFunc func(args []string) []byte

var commands = make(map[string]HandlerFunc)

func Register(name string, f HandlerFunc) {
	commands[name] = f
}

func Dispatch(cmd *Command) []byte {
	handler, ok := commands[cmd.Name]
	if !ok {
		return core.EncodeError(fmt.Sprintf("ERR unknown command '%s'", cmd.Name))
	}
	return handler(cmd.Args)
}
