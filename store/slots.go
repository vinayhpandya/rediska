package store

import "github.com/vinayhpandya/rediska/cluster"

type slotIndex [cluster.NumSlots]map[string]struct{}

// EnableSlotIndex turns the index on. Called once at startup in cluster mode.
func (s *Store) EnableSlotIndex() {
	s.slots = new(slotIndex)
	for key := range s.data {
		s.indexAdd(key)
	}
}

// keyAdded does the bookkeeping for a brand-new key: it bumps the key
// count for INFO and indexes the key. Every place that creates a key must
// call it, just as every deletion goes through removeKey.
func (s *Store) keyAdded(key string) {
	UpdateDbStat(0, "keys", 1)
	s.indexAdd(key)
}

func (s *Store) indexAdd(key string) {
	if s.slots == nil {
		return // Cluster mode is turned off
	}
	slot := cluster.KeyHashSlot(key)
	if s.slots[slot] == nil {
		s.slots[slot] = make(map[string]struct{})
	}
	s.slots[slot][key] = struct{}{}
}

func (s *Store) indexRemove(key string) {
	if s.slots == nil {
		return
	}
	slot := cluster.KeyHashSlot(key)
	delete(s.slots[slot], key)
	if len(s.slots[slot]) == 0 {
		s.slots[slot] = nil // free the map once the slot is empty
	}
}

// CountKeysInSlot returns how many keys this store holds in slot.
func (s *Store) CountKeysInSlot(slot int) int {
	if s.slots == nil || slot < 0 || slot >= cluster.NumSlots {
		return 0
	}
	return len(s.slots[slot])
}

// KeysInSlot returns up to count keys from slot, in no particular order.
func (s *Store) KeysInSlot(slot, count int) []string {
	if s.slots == nil || slot < 0 || slot >= cluster.NumSlots || count <= 0 {
		return nil
	}
	keys := make([]string, 0, min(count, len(s.slots[slot])))
	for key := range s.slots[slot] {
		if len(keys) == count {
			break
		}
		keys = append(keys, key)
	}
	return keys
}
