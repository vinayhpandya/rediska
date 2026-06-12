package store

import "fmt"

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
			delete(s.data, key)
			fmt.Printf("active expiry: deleted key %q", key)
			deleted++
		}
	}
	return deleted
}
