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
}

// Store is not thread-safe.
// All access must go through the single command worker in server.go.
type Store struct {
	data map[string]entry
	now  func() time.Time // injectable for testing e.g. fake clock
}

func New() *Store {
	return &Store{
		data: make(map[string]entry),
		now:  time.Now,
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
	_, exists := s.data[key]
	if !exists {
		UpdateDbStat(0, "keys", 1)
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
	s.data[key] = entry{
		encoding:  enc,
		value:     storedValue,
		expiresAt: expiresAt,
	}
}

func (s *Store) GetInt(key string) (int64, bool, error, time.Time) {
	e, ok := s.data[key]
	if !ok {
		return 0, false, nil, time.Time{}
	}
	if s.isExpired(e) {
		delete(s.data, key)
		return 0, false, nil, time.Time{}
	}
	if e.encoding != EncodingINT {
		return 0, true, fmt.Errorf("ERR value is not an integer or out of range"), time.Time{}
	}
	return e.value.(int64), true, nil, e.expiresAt
}
func (s *Store) GetEncoding(key string) (Encoding, bool) {
	value, ok := s.data[key]
	if !ok {
		return 0, false
	}
	if s.isExpired(value) {
		// inline deletion — no separate deleteIfExpired needed
		// previously this caused a deadlock by trying to acquire a lock already held
		delete(s.data, key)
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
		// inline deletion — no separate deleteIfExpired needed
		// previously this caused a deadlock by trying to acquire a lock already held
		UpdateDbStat(0, "keys", -1)
		delete(s.data, key)
		return "", false
	}
	switch e.encoding {
	case EncodingINT:
		return strconv.FormatInt(e.value.(int64), 10), true
	case EncodingRAW, EncodingEMBSTR:
		return e.value.(string), true
	}
	return "", false
}

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
		// inline deletion — same fix as Get
		UpdateDbStat(0, "keys", -1)
		delete(s.data, key)
		return -2
	}
	return (remaining.Milliseconds() + 500) / 1000
}

func (s *Store) isExpired(e entry) bool {
	// zero expiresAt means no expiry set
	// now().Before(expiresAt) means it hasn't expired yet
	return !e.expiresAt.IsZero() && !s.now().Before(e.expiresAt)
}

// deleteIfExpired is removed entirely.
// it was causing deadlocks by trying to acquire a lock already held by Get/TTL.
// deletion is now inlined at the point where expiry is detected.
