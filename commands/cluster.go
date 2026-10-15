package commands

import (
	"fmt"
	"strings"

	"rediska/cluster"
	"rediska/config"
	"rediska/core"
)

func init() {
	Register("CLUSTER", handleCluster)
}

// handleCluster dispatches CLUSTER subcommands. Error replies match
// Redis 7 word for word, so clients behave the same against either server.
func handleCluster(args []string) []byte {
	if !config.ClusterEnabled {
		return core.EncodeError("ERR This instance has cluster support disabled")
	}
	if len(args) == 0 {
		return core.EncodeError("ERR wrong number of arguments for 'cluster' command")
	}
	switch strings.ToUpper(args[0]) {
	case "KEYSLOT":
		if len(args) != 2 {
			return core.EncodeError("ERR wrong number of arguments for 'cluster|keyslot' command")
		}
		return core.EncodeInteger(int64(cluster.KeyHashSlot(args[1])))
	default:
		return core.EncodeError(fmt.Sprintf("ERR unknown subcommand '%s'. Try CLUSTER HELP.", args[0]))
	}
}
