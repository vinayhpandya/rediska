package store

import (
	"time"
	// sync is gone — no locks needed
)

type entry struct {
	value     string
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

func (s *Store) Set(key, value string, ttl time.Duration) {
	var expiresAt time.Time
	if ttl > 0 {
		expiresAt = s.now().Add(ttl)
	}
	s.data[key] = entry{value: value, expiresAt: expiresAt}
}

func (s *Store) Get(key string) (string, bool) {
	e, ok := s.data[key]
	if !ok {
		return "", false
	}
	if s.isExpired(e) {
		// inline deletion — no separate deleteIfExpired needed
		// previously this caused a deadlock by trying to acquire a lock already held
		delete(s.data, key)
		return "", false
	}
	return e.value, true
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
