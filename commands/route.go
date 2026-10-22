package commands

import (
	"fmt"
	"github.com/vinayhpandya/rediska/cluster"
	"github.com/vinayhpandya/rediska/core"
)

// checkCluster decides whether this node may run cmd. It returns nil if the
// command can run here, or the error reply to send instead (see checkKeys).
func checkCluster(cmd *Command) []byte {
	keys, _ := KeysOf(cmd)
	return checkKeys(keys)
}

// CheckCluster reports whether this node may run cmd in cluster mode. It
// returns nil if so (or if cluster mode is off), otherwise the error reply
// to send instead: CROSSSLOT, CLUSTERDOWN or MOVED.
//
// The server package calls it while queuing commands inside MULTI. That
// happens on the connection's goroutine, not the dispatch worker, which is
// safe only because the cluster layout never changes after startup. Once
// slots can move at runtime, this must go through the worker or take a lock.
func CheckCluster(cmd *Command) []byte {
	return checkCluster(cmd)
}

// checkClusterBatch checks a whole MULTI/EXEC transaction: the keys of all
// queued commands together must be in one slot that this node owns.
func checkClusterBatch(cmds []*Command) []byte {
	var keys []string
	for _, c := range cmds {
		k, _ := KeysOf(c)
		keys = append(keys, k...)
	}
	return checkKeys(keys)
}

// checkKeys is the cluster rule for a list of keys, checked in the same
// order as real Redis. It returns nil if they can be served here, or:
//
//	CROSSSLOT    the keys are in different slots
//	CLUSTERDOWN  no node owns the keys' slot
//	MOVED        another node owns the slot; the reply says which one
//
// The error texts match Redis 7 word for word, so redis-cli -c and cluster
// client libraries understand them.
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
