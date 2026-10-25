// Package store holds Rediska's data: an in-memory map from keys to
// values, with expiry, memory accounting and eviction (LRU or LFU).
//
// Values are kept in compact encodings, as in Redis: integers as numbers,
// strings as SDS, HyperLogLogs as sketches (see Encoding).
//
// A Store is not safe for concurrent use. The server only touches Default
// from its single dispatch worker goroutine.
package store

import (
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/vinayhpandya/rediska/core/sds"
	"github.com/vinayhpandya/rediska/store/hll"
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
	slots      *slotIndex
}

// New returns an empty Store using the noeviction policy.
func New() *Store {
	return &Store{
		data:   make(map[string]entry),
		now:    time.Now,
		evPool: make([]evictionCandidate, 0, EvictionPoolSize),
		policy: "noeviction",
	}
}

// SetPolicy sets the maxmemory policy, which decides how keys are evicted
// and whether keys track recency (LRU) or frequency (LFU). It is called
// once at startup.
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

// ErrNotHLL is returned when a HyperLogLog command is used on a key that
// holds some other kind of value. The text matches Redis's reply.
var ErrNotHLL = errors.New("WRONGTYPE Key is not a valid HyperLogLog string value.")

// ErrWrongType is returned when a command is used on a key that holds the
// wrong kind of value. The text matches Redis's reply.
var ErrWrongType = errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")

// lookup returns key's entry, or false if there is none. A key that has
// expired is removed first and reported as missing (lazy expiry), so
// callers never see a stale value.
func (s *Store) lookup(key string) (entry, bool) {
	e, ok := s.data[key]
	if ok && s.isExpired(e) {
		s.removeKey(key)
		return entry{}, false
	}
	return e, ok
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

// Set stores value under key, replacing any old value and its expiry. If
// ttl is greater than zero the key expires after ttl; otherwise it never
// expires. The value is stored as an integer if it parses as one, and as a
// string otherwise.
func (s *Store) Set(key, value string, ttl time.Duration) {
	old, exists := s.data[key]
	if !exists {
		s.keyAdded(key)
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
//
// An expired key counts as missing. A key holding a HyperLogLog is left
// unchanged and ErrWrongType is returned.
func (s *Store) Append(key, value string) (int64, error) {
	old, exists := s.lookup(key)
	if !exists {
		s.Set(key, value, 0)
		return int64(len(value)), nil
	}
	if old.encoding == EncodingHLL {
		// A HyperLogLog has no string to append to (strVal is nil).
		return 0, ErrWrongType
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
	return int64(buf.Len()), nil
}

// GetInt returns key's value as an integer, for commands like INCR. The
// results are the value, whether the key exists, an error if it exists
// but doesn't hold an integer, and the key's expiry time (zero if none).
// It counts as an access for LRU/LFU.
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

// Get returns key's value as a string, and whether the key exists. An
// expired key is removed and reported as missing. It counts as an access
// for LRU/LFU. A key holding a HyperLogLog is also reported as missing,
// whereas real Redis would return its raw bytes.
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

// UsedMemory returns the store's estimate of the memory its keys and
// values use, in bytes. It is what --maxmemory is compared against.
func (s *Store) UsedMemory() int64 {
	return s.usedMemory
}

// KeyCount returns the number of keys currently in the store, including any
// that are logically expired but not yet removed.
func (s *Store) KeyCount() int {
	return len(s.data)
}

// PFAdd adds values to the HyperLogLog at key, creating it if needed. It
// reports whether anything changed: creating the key counts as a change,
// even with no values, as in Redis. If key holds some other kind of value,
// PFAdd changes nothing and returns ErrNotHLL.
func (s *Store) PFAdd(key string, values ...string) (changed bool, err error) {
	e, exists := s.data[key]
	if exists && s.isExpired(e) {
		s.removeKey(key)
		exists = false
	}
	if exists && e.encoding != EncodingHLL {
		return false, ErrNotHLL
	}
	if exists {
		// Adding values can grow the sketch, so drop its old size first
		// and add the new size back below.
		s.usedMemory -= entrySize(key, e)
		e.lru = s.stampAccess(e.lru)
	} else {
		e = entry{encoding: EncodingHLL, hllVal: hll.New(), lru: s.stampNew()}
		s.keyAdded(key)
		changed = true
	}
	for _, v := range values {
		if e.hllVal.Add([]byte(v)) {
			changed = true
		}
	}
	s.data[key] = e
	s.usedMemory += entrySize(key, e)
	return changed, nil
}

// PFCount returns the estimated number of distinct values in the
// HyperLogLogs at keys. With one key, that's its own count; with several,
// it's the count of their union, computed in a temporary sketch without
// storing anything, as in Redis. Missing or expired keys count as empty.
// If any key holds some other kind of value, it returns ErrNotHLL.
func (s *Store) PFCount(keys ...string) (int64, error) {
	if len(keys) == 1 {
		e, ok := s.lookup(keys[0])
		if !ok {
			return 0, nil
		}
		if e.encoding != EncodingHLL {
			return 0, ErrNotHLL
		}
		return int64(e.hllVal.Count()), nil
	}
	union := hll.New()
	for _, key := range keys {
		e, ok := s.lookup(key)
		if !ok {
			continue
		}
		if e.encoding != EncodingHLL {
			return 0, ErrNotHLL
		}
		union.Merge(e.hllVal)
	}
	return int64(union.Count()), nil
}

// PFMerge stores in dest the union of the HyperLogLogs at dest and every
// source key. Missing or expired keys count as empty. If dest already
// exists it keeps its expiry, as in Redis.
//
// Every key is checked before anything changes: if any of them holds some
// other kind of value, PFMerge returns ErrNotHLL and changes nothing.
func (s *Store) PFMerge(dest string, sources ...string) error {
	allKeys := append([]string{dest}, sources...)
	for _, key := range allKeys {
		if e, ok := s.lookup(key); ok && e.encoding != EncodingHLL {
			return ErrNotHLL
		}
	}

	acc := hll.New()
	for _, key := range allKeys {
		if e, ok := s.data[key]; ok { // lookup above removed expired keys
			acc.Merge(e.hllVal)
		}
	}

	var expiresAt time.Time
	old, existed := s.data[dest]
	if existed {
		s.usedMemory -= entrySize(dest, old)
		expiresAt = old.expiresAt
	} else {
		s.keyAdded(dest)
	}

	e := entry{
		encoding:  EncodingHLL,
		hllVal:    acc,
		expiresAt: expiresAt,
		lru:       s.stampNew(),
	}
	s.data[dest] = e
	s.usedMemory += entrySize(dest, e)
	return nil
}

// Delete removes keys and returns how many of them existed. Keys that had
// already expired are removed too, but not counted, as in Redis.
func (s *Store) Delete(keys ...string) int64 {
	var removed int64
	for _, key := range keys {
		e, ok := s.data[key]
		if !ok {
			continue
		}
		if !s.isExpired(e) {
			removed++
		}
		s.removeKey(key)
	}
	return removed
}

// Note: all deletions now route through removeKey (in lru.go) so the
// usedMemory counter and the keys stat stay consistent. The old inline
// delete(s.data, key) calls have been replaced.
