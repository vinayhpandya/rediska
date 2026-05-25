package store

import (
	"sync"
	"time"
)

type entry struct {
	value     string
	expiresAt time.Time
}

type Store struct {
	mu   sync.RWMutex
	data map[string]entry
	now  func() time.Time
}

func New() *Store {
	return &Store{
		data: make(map[string]entry),
		now:  time.Now,
	}
}

var Default = New()

func (s *Store) Set(key, value string, ttl time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var expiresAt time.Time
	if ttl > 0 {
		expiresAt = s.now().Add(ttl)
	}
	s.data[key] = entry{value: value, expiresAt: expiresAt}
}
func (s *Store) Get(key string) (string, bool) {
	s.mu.Lock()
	e, ok := s.data[key]
	defer s.mu.Unlock()
	if !ok {
		return "", false
	}
	if s.isExpired(e) {
		s.deleteIfExpired(key)
		return "", false
	}
	return e.value, true
}

func (s *Store) TTL(key string) int64 {
	s.mu.RLock()
	e, ok := s.data[key]
	s.mu.RUnlock()
	if !ok {
		return -2
	}
	if e.expiresAt.IsZero() {
		return -1
	}
	remaining := e.expiresAt.Sub(s.now())
	if remaining <= 0 {
		s.deleteIfExpired(key)
		return -2
	}
	return (remaining.Milliseconds() + 500) / 1000
}
func (s *Store) isExpired(e entry) bool {
	return !e.expiresAt.IsZero() && !s.now().Before(e.expiresAt)
}

func (s *Store) deleteIfExpired(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e, ok := s.data[key]; ok && s.isExpired(e) {
		delete(s.data, key)
	}
}
