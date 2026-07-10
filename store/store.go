package store

import (
	"fmt"
	"strconv"
	"time"

	"rediska/core/sds"
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
	e := entry{
		encoding:  enc,
		expiresAt: expiresAt,
		lru:       s.lruClock(), // a write counts as an access
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
		lru:       s.lruClock(),
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
	e.lru = s.lruClock()
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
	e.lru = s.lruClock()
	s.data[key] = e // reassign to persist the stamp (value-type map)
	switch e.encoding {
	case EncodingINT:
		return strconv.FormatInt(e.intVal, 10), true
	case EncodingRAW, EncodingEMBSTR:
		return e.strVal.String(), true
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
