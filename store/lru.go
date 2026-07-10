package store

import "sort"

// LRU clock constants. The clock is a 24-bit value that ticks once per
// second and wraps back to zero roughly every 194 days, mirroring Redis.
const (
	LRUClockMax        = (1 << 24) - 1 // 16777215
	LRUClockResolution = 1000          // milliseconds per tick (1 second)
	EvictionPoolSize   = 16            // candidates kept across eviction cycles
	entryOverhead      = 64            // estimated per-key bytes (map bucket + headers); tune later
)

// evictionCandidate is one slot in the eviction pool: a key plus the idle
// time it had when it was last sampled.
type evictionCandidate struct {
	key  string
	idle uint32
}

// lruClock returns the current 24-bit clock value, derived from the
// injectable now() so tests stay deterministic.
func (s *Store) lruClock() uint32 {
	return uint32((s.now().UnixMilli() / LRUClockResolution) & LRUClockMax)
}

// lruIdle returns how many ticks elapsed between when a key was stamped
// (access) and the current clock, handling a single wraparound the long
// way round the clock face.
func lruIdle(clock, access uint32) uint32 {
	if clock >= access {
		return clock - access
	}
	return clock + (LRUClockMax - access)
}

// entrySize estimates the bytes a key/entry pair occupies.
func entrySize(key string, e entry) int64 {
	valueSize := 0
	switch e.encoding {
	case EncodingINT:
		valueSize = 8
	case EncodingEMBSTR, EncodingRAW:
		if e.strVal != nil {
			valueSize = e.strVal.Cap()
		}
	}
	return int64(len(key) + valueSize + entryOverhead)
}

// removeKey is the single deletion path: it keeps the memory counter and
// the key stat in sync. Every delete must go through here.
func (s *Store) removeKey(key string) {
	e, ok := s.data[key]
	if !ok {
		return
	}
	s.usedMemory -= entrySize(key, e)
	delete(s.data, key)
	UpdateDbStat(0, "keys", -1)
}

// OverMemory reports whether the store is above the byte limit.
func (s *Store) OverMemory(maxBytes int64) bool {
	return maxBytes > 0 && s.usedMemory > maxBytes
}

// sampleKeys grabs up to n keys. Go randomizes map iteration order, so
// taking the first n is a fair random sample.
func (s *Store) sampleKeys(n int) []string {
	keys := make([]string, 0, n)
	for k := range s.data {
		keys = append(keys, k)
		if len(keys) >= n {
			break
		}
	}
	return keys
}

// poolInsert merges one candidate into the pool, which is kept sorted
// ascending by idle (freshest first, stalest last). The pool is sticky
// toward staleness: when full, a candidate fresher than every pooled key
// is dropped, so good (stale) candidates survive across cycles.
func (s *Store) poolInsert(key string, idle uint32) {
	for i := range s.evPool {
		if s.evPool[i].key == key {
			s.evPool[i].idle = idle // refresh an existing entry
			s.sortPool()
			return
		}
	}
	cand := evictionCandidate{key: key, idle: idle}
	if len(s.evPool) < EvictionPoolSize {
		s.evPool = append(s.evPool, cand)
		s.sortPool()
		return
	}
	if idle <= s.evPool[0].idle {
		return // fresher than everything pooled; not worth keeping
	}
	s.evPool[0] = cand // displace the freshest pooled candidate
	s.sortPool()
}

func (s *Store) sortPool() {
	sort.Slice(s.evPool, func(i, j int) bool {
		return s.evPool[i].idle < s.evPool[j].idle
	})
}

// PerformEvictions frees keys until the store is back under maxBytes.
// It runs only inside the single command worker, so it needs no locks.
func (s *Store) PerformEvictions(samples int, maxBytes int64) {
	if maxBytes <= 0 {
		return
	}
	if samples <= 0 {
		samples = 5
	}
	for s.usedMemory > maxBytes {
		if len(s.data) == 0 {
			return // nothing left to free
		}
		clock := s.lruClock()
		for _, k := range s.sampleKeys(samples) {
			e := s.data[k]
			s.poolInsert(k, lruIdle(clock, e.lru))
		}
		// Evict from the stale end of the pool, dropping slots whose keys
		// have since disappeared until we hit one that still exists.
		for i := len(s.evPool) - 1; i >= 0; i-- {
			cand := s.evPool[i]
			s.evPool = append(s.evPool[:i], s.evPool[i+1:]...)
			if _, ok := s.data[cand.key]; ok {
				s.removeKey(cand.key)
				break
			}
		}
	}
}
