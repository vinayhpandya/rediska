package commands

import (
	"fmt"
	"rediska/cluster"
	"rediska/core"
)

func checkCluster(cmd *Command) []byte {
	layout := cluster.Current
	if layout == nil {
		return nil // cluster mode is off
	}

	keys, _ := KeysOf(cmd)
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
