package commands

import (
	"fmt"
	"github.com/vinayhpandya/rediska/cluster"
	"github.com/vinayhpandya/rediska/core"
)

func checkCluster(cmd *Command) []byte {
	keys, _ := KeysOf(cmd)
	return checkKeys(keys)
}

func CheckCluster(cmd *Command) []byte {
	return checkCluster(cmd)
}

func checkClusterBatch(cmds []*Command) []byte {
	var keys []string
	for _, c := range cmds {
		k, _ := KeysOf(c)
		keys = append(keys, k...)
	}
	return checkKeys(keys)
}

func checkKeys(keys []string) []byte {
	layout := cluster.Current
	if layout == nil {
		return nil // cluster mode is off
	}

	if len(keys) == 0 {
		return nil // PING, INFO, CLUSTER ...: always run locally
	}

	// ② All keys must be in one slot.
	slot := cluster.KeyHashSlot(keys[0])
	for _, k := range keys[1:] {
		if cluster.KeyHashSlot(k) != slot {
			return core.EncodeError("CROSSSLOT Keys in request don't hash to the same slot")
		}
	}

	// ③ Somebody must own that slot.
	owner := layout.Owner(slot)
	if owner == nil {
		return core.EncodeError("CLUSTERDOWN Hash slot not served")
	}

	// ④ And it must be me.
	if owner != layout.Self {
		return core.EncodeError(fmt.Sprintf("MOVED %d %s", slot, owner.Addr))
	}
	return nil
}
