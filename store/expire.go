package store

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
