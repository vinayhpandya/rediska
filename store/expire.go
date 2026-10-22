package store

// DeleteExpired does one round of active expiry: it checks up to 20 keys
// that have an expiry, picked in Go's random map order, removes those that
// have expired, and returns how many it removed. The server runs it every
// 100 ms and repeats while a round still finds 5 or more expired keys.
func (s *Store) DeleteExpired() int {
	const maxKeys = 20
	deleted := 0
	checked := 0

	for key, e := range s.data {
		if checked >= maxKeys {
			break
		}
		if e.expiresAt.IsZero() {
			continue
		}
		checked++
		if s.isExpired(e) {
			s.removeKey(key)
			deleted++
		}
	}
	return deleted
}
