package store

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"strconv"
	"time"

	"github.com/vinayhpandya/rediska/store/hll"
)

// SlotCommands returns the commands that recreate every key in slot, with
// its value and absolute expiry time:
//
//	SET key value [PXAT unix-ms]
//	_HLLRESTORE key <raw sketch> [PXAT unix-ms]
//
// Keys that have expired but not been removed yet are left out. It only
// reads: nothing is removed or counted as an access. Replication sends a
// replica these, one slot at a time, as the snapshot of the master's data.
// It needs the slot index, so it only works in cluster mode.
func (s *Store) SlotCommands(slot int) [][]string {
	if s.slots == nil || slot < 0 || slot >= len(s.slots) {
		return nil
	}
	var cmds [][]string
	for key := range s.slots[slot] {
		e, ok := s.data[key]
		if !ok || s.isExpired(e) {
			continue
		}
		var cmd []string
		switch e.encoding {
		case EncodingINT:
			cmd = []string{"SET", key, strconv.FormatInt(e.intVal, 10)}
		case EncodingEMBSTR, EncodingRAW:
			cmd = []string{"SET", key, e.strVal.String()}
		case EncodingHLL:
			cmd = []string{"_HLLRESTORE", key, string(e.hllVal.Bytes())}
		default:
			continue
		}
		if !e.expiresAt.IsZero() {
			cmd = append(cmd, "PXAT", strconv.FormatInt(e.expiresAt.UnixMilli(), 10))
		}
		cmds = append(cmds, cmd)
	}
	return cmds
}

// RestoreHLL stores the HyperLogLog that data describes (see hll.Bytes)
// under key, replacing any old value. A zero expiresAt means no expiry;
// a time that has already passed deletes the key instead.
func (s *Store) RestoreHLL(key string, data []byte, expiresAt time.Time) error {
	h, err := hll.FromBytes(data)
	if err != nil {
		return err
	}
	if !expiresAt.IsZero() && !s.now().Before(expiresAt) {
		s.removeKey(key)
		return nil
	}
	old, exists := s.data[key]
	if !exists {
		s.keyAdded(key)
	} else {
		s.usedMemory -= entrySize(key, old)
	}
	e := entry{encoding: EncodingHLL, hllVal: h, expiresAt: expiresAt, lru: s.stampNew()}
	s.data[key] = e
	s.usedMemory += entrySize(key, e)
	return nil
}

// Flush deletes every key at once. A replica calls it before loading a
// fresh copy of its master's data. Deletions aren't tracked: they aren't
// changes anyone else needs to hear about.
func (s *Store) Flush() {
	UpdateDbStat(0, "keys", -len(s.data))
	s.data = make(map[string]entry)
	s.usedMemory = 0
	s.evPool = s.evPool[:0]
	s.deleted = nil
	if s.slots != nil {
		s.slots = new(slotIndex)
	}
}

// Digest returns a fingerprint of every live key: its name, value and
// expiry time. Two stores with the same data have the same digest, in
// whatever order the keys were added. DEBUG DIGEST shows it, so tests can
// check that a replica holds exactly its master's data.
func (s *Store) Digest() string {
	var sum [sha1.Size]byte
	for key, e := range s.data {
		if s.isExpired(e) {
			continue
		}
		// The type, not the encoding: a string APPEND made RAW comes back
		// EMBSTR from a SET, and it's still the same data.
		var typ, value string
		switch e.encoding {
		case EncodingINT:
			typ, value = "string", strconv.FormatInt(e.intVal, 10)
		case EncodingEMBSTR, EncodingRAW:
			typ, value = "string", e.strVal.String()
		case EncodingHLL:
			typ, value = "hll", string(e.hllVal.Bytes())
		}
		var expires int64
		if !e.expiresAt.IsZero() {
			expires = e.expiresAt.UnixMilli()
		}
		h := sha1.Sum([]byte(fmt.Sprintf("%d:%s:%s:%d:%s:%d", len(key), key, typ, len(value), value, expires)))
		for i := range sum {
			sum[i] ^= h[i] // XOR: the order keys are visited in doesn't matter
		}
	}
	return hex.EncodeToString(sum[:])
}
