package store

import (
	"fmt"
	"strconv"
	"time"
	// sync is gone — no locks needed
)

type entry struct {
	encoding  Encoding
	value     any
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
}

func New() *Store {
	return &Store{
		data:   make(map[string]entry),
		now:    time.Now,
		evPool: make([]evictionCandidate, 0, EvictionPoolSize),
	}
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
	var storedValue any
	switch enc {
	case EncodingINT:
		n, _ := strconv.ParseInt(value, 10, 64)
		storedValue = n
	case EncodingEMBSTR, EncodingRAW:
		storedValue = value
	}
	e := entry{
		encoding:  enc,
		value:     storedValue,
		expiresAt: expiresAt,
		lru:       s.lruClock(), // a write counts as an access
	}
	s.data[key] = e
	s.usedMemory += entrySize(key, e)
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
	e.lru = s.lruClock()
	s.data[key] = e // reassign to persist the stamp (value-type map)
	if e.encoding != EncodingINT {
		return 0, true, fmt.Errorf("ERR value is not an integer or out of range"), time.Time{}
	}
	return e.value.(int64), true, nil, e.expiresAt
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
	e.lru = s.lruClock()
	s.data[key] = e // reassign to persist the stamp (value-type map)
	switch e.encoding {
	case EncodingINT:
		return strconv.FormatInt(e.value.(int64), 10), true
	case EncodingRAW, EncodingEMBSTR:
		return e.value.(string), true
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

// Note: all deletions now route through removeKey (in lru.go) so the
// usedMemory counter and the keys stat stay consistent. The old inline
// delete(s.data, key) calls have been replaced.
