package commands

import (
	"strconv"
	"strings"
	"time"

	"rediska/core"
	"rediska/store"
)

func init() {
	Register("SET", handleSet)
}

func handleSet(args []string) []byte {
	if len(args) != 2 && len(args) != 4 {
		return core.EncodeError("ERR wrong number of arguments for 'set' command")
	}

	key := args[0]
	value := args[1]

	var ttl time.Duration

	if len(args) == 4 {
		option := strings.ToUpper(args[2])
		n, err := strconv.Atoi(args[3])
		if err != nil || n <= 0 {
			return core.EncodeError("ERR invalid expire time in 'set' command")
		}

		switch option {
		case "EX":
			ttl = time.Duration(n) * time.Second
		case "PX":
			ttl = time.Duration(n) * time.Millisecond
		default:
			return core.EncodeError("ERR syntax error")
		}
	}

	store.Default.Set(key, value, ttl)

	return core.EncodeSimpleString("OK")
}
