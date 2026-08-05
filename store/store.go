package store

import (
	"fmt"
	"strconv"
	"time"

	"rediska/core/sds"
	"rediska/store/hll"
	// sync is gone — no locks needed
)

// entry is a tagged union: `encoding` says which of intVal/strVal is
// actually populated. This avoids boxing values into `any` (which costs a
// hidden heap allocation for intVal on every Set, plus a 16-byte interface
// header) in favor of plain fields the compiler can check directly.
type entry struct {
	encoding  Encoding
	intVal    int64
	strVal    *sds.SDS
	hllVal    *hll.HLL
	expiresAt time.Time
	lru       uint32 // 24-bit LRU clock stamp; see lru.go
}

// Store is not thread-safe.
// All access must go through the single command worker in server.go.
type Store struct {
	data       map[string]entry
	now        func() time.Time    // injectable for testing e.g. fake clock
	usedMemory int64               // running byte estimate, kept in sync by Set/removeKey
	evPool     []evictionCandidate // eviction pool, persists across cycles
	policy     string              // active maxmemory-policy; see SetPolicy
}

func New() *Store {
	return &Store{
		data:   make(map[string]entry),
		now:    time.Now,
		evPool: make([]evictionCandidate, 0, EvictionPoolSize),
		policy: "noeviction",
	}
}

func (s *Store) SetPolicy(policy string) {
	s.policy = policy
}

func (s *Store) isLFU() bool {
	return s.policy == "allkeys-lfu" || s.policy == "volatile-lfu"
}

// stampNew returns the lru field value for a brand-new entry: either an
// LRU clock stamp or a fresh LFU packed field, depending on policy.
func (s *Store) stampNew() uint32 {
	if s.isLFU() {
		return lfuNew(s.lfuClock())
	}
	return s.lruClock()
}

func (s *Store) stampAccess(current uint32) uint32 {
	if s.isLFU() {
		return lfuTouch(current, s.lfuClock())
	}
	return s.lruClock()
}

// Default is the global store instance.
// Never access this directly — always go through the command worker.
var Default = New()

func detectEncoding(value string) Encoding {
	if _, err := strconv.ParseInt(value, 10, 64); err == nil {
		return EncodingINT
	}
	if len(value) <= 44 {
		return EncodingEMBSTR
	}
	return EncodingRAW
}

// GetFreq returns a key's LFU counter (used by OBJECT FREQ), same as real
// Redis. It's a metadata read and does NOT bump recency. ok is false if
// the key doesn't exist, and policyOK is false if the active policy isn't
// an LFU policy -- callers should surface that as a distinct error,
// mirroring Redis's "An LFU maxmemory policy is not selected" response.
func (s *Store) GetFreq(key string) (freq uint8, ok bool, policyOK bool) {
	if !s.isLFU() {
		return 0, false, false
	}
	value, exists := s.data[key]
	if !exists {
		return 0, false, true
	}
	if s.isExpired(value) {
		s.removeKey(key)
		return 0, false, true
	}
	return lfuDecodeCounter(value.lru), true, true
}

func (s *Store) Set(key, value string, ttl time.Duration) {
	old, exists := s.data[key]
	if !exists {
		UpdateDbStat(0, "keys", 1)
	} else {
		// overwriting: drop the old size before adding the new one
		s.usedMemory -= entrySize(key, old)
	}
	var expiresAt time.Time
	if ttl > 0 {
		expiresAt = s.now().Add(ttl)
	}
	enc := detectEncoding(value)
	e := entry{
		encoding:  enc,
		expiresAt: expiresAt,
		lru:       s.stampNew(), // a write counts as an access
	}
	switch enc {
	case EncodingINT:
		n, _ := strconv.ParseInt(value, 10, 64)
		e.intVal = n
	case EncodingEMBSTR, EncodingRAW:
		e.strVal = sds.New(value)
	}
	s.data[key] = e
	s.usedMemory += entrySize(key, e)
}

// Append implements Redis's APPEND: creates the key if it doesn't exist
// (identical to a SET with no TTL), otherwise appends to the existing
// value. An INT-encoded key is converted to its string form first and
// becomes RAW-encoded afterward — same as real Redis. Returns the new
// total length, matching Redis's APPEND reply.
func (s *Store) Append(key, value string) int64 {
	old, exists := s.data[key]
	if !exists {
		s.Set(key, value, 0)
		return int64(len(value))
	}

	s.usedMemory -= entrySize(key, old)

	var buf *sds.SDS
	if old.encoding == EncodingINT {
		buf = sds.New(strconv.FormatInt(old.intVal, 10))
	} else {
		buf = old.strVal
	}
	buf.AppendString(value)

	e := entry{
		encoding:  EncodingRAW,
		strVal:    buf,
		expiresAt: old.expiresAt, // APPEND preserves existing TTL
		lru:       s.stampAccess(old.lru),
	}
	s.data[key] = e
	s.usedMemory += entrySize(key, e)
	return int64(buf.Len())
}

func (s *Store) GetInt(key string) (int64, bool, error, time.Time) {
	e, ok := s.data[key]
	if !ok {
		return 0, false, nil, time.Time{}
	}
	if s.isExpired(e) {
		s.removeKey(key)
		return 0, false, nil, time.Time{}
	}
	// stamp recency: the key was accessed
	e.lru = s.stampAccess(e.lru)
	s.data[key] = e // reassign to persist the stamp (value-type map)
	if e.encoding != EncodingINT {
		return 0, true, fmt.Errorf("ERR value is not an integer or out of range"), time.Time{}
	}
	return e.intVal, true, nil, e.expiresAt
}

// GetEncoding is a metadata read (used by OBJECT ENCODING) and deliberately
// does NOT bump recency — inspecting a key shouldn't make it look hot.
func (s *Store) GetEncoding(key string) (Encoding, bool) {
	value, ok := s.data[key]
	if !ok {
		return 0, false
	}
	if s.isExpired(value) {
		s.removeKey(key)
		return 0, false
	}
	return value.encoding, true
}

func (s *Store) Get(key string) (string, bool) {
	e, ok := s.data[key]
	if !ok {
		return "", false
	}
	if s.isExpired(e) {
		s.removeKey(key)
		return "", false
	}
	// stamp recency: the key was accessed
	e.lru = s.stampAccess(e.lru)
	s.data[key] = e // reassign to persist the stamp (value-type map)
	switch e.encoding {
	case EncodingINT:
		return strconv.FormatInt(e.intVal, 10), true
	case EncodingRAW, EncodingEMBSTR:
		return e.strVal.String(), true
	case EncodingHLL:
		return "", false
	}
	return "", false
}

// TTL is a metadata read and does NOT bump recency, matching Redis.
func (s *Store) TTL(key string) int64 {
	e, ok := s.data[key]
	if !ok {
		return -2 // key does not exist
	}
	if e.expiresAt.IsZero() {
		return -1 // key exists but has no expiry
	}
	remaining := e.expiresAt.Sub(s.now())
	if remaining <= 0 {
		s.removeKey(key)
		return -2
	}
	return (remaining.Milliseconds() + 500) / 1000
}

func (s *Store) isExpired(e entry) bool {
	// zero expiresAt means no expiry set
	// now().Before(expiresAt) means it hasn't expired yet
	return !e.expiresAt.IsZero() && !s.now().Before(e.expiresAt)
}

func (s *Store) UsedMemory() int64 {
	return s.usedMemory
}

// KeyCount returns the number of keys currently in the store, including any
// that are logically expired but not yet removed.
func (s *Store) KeyCount() int {
	return len(s.data)
}

func (s *Store) PFAdd(key string, values ...string) (changed bool) {
	e, exists := s.data[key]
	if !exists || e.encoding != EncodingHLL {
		e = entry{encoding: EncodingHLL, hllVal: hll.New(), lru: s.lruClock()}
	}
	for _, v := range values {
		if e.hllVal.Add([]byte(v)) {
			changed = true
		}
	}
	s.data[key] = e
	return changed
}

func (s *Store) PFCount(key string) int64 {
	e, ok := s.data[key]
	if !ok || e.encoding != EncodingHLL {
		return 0
	}
	return int64(e.hllVal.Count())
}

func (s *Store) PFMerge(dest string, sources ...string) {
	acc := hll.New()

	allKeys := append([]string{dest}, sources...)
	for _, key := range allKeys {
		e, ok := s.data[key]
		if !ok || e.encoding != EncodingHLL {
			continue
		}
		acc.Merge(e.hllVal)
	}

	old, existed := s.data[dest]
	if existed {
		s.usedMemory -= entrySize(dest, old)
	} else {
		UpdateDbStat(0, "keys", 1)
	}

	e := entry{
		encoding: EncodingHLL,
		hllVal:   acc,
		lru:      s.stampNew(),
		// PFMERGE does not preserve dest's old TTL -- a fresh union
		// value, like a plain overwrite via Set.
	}
	s.data[dest] = e
	s.usedMemory += entrySize(dest, e)
}

// Note: all deletions now route through removeKey (in lru.go) so the
// usedMemory counter and the keys stat stay consistent. The old inline
// delete(s.data, key) calls have been replaced.
