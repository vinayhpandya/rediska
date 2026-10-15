// Package cluster holds Redis Cluster logic: hash slots now, and later the
// slot map, redirects and the cluster bus.
package cluster

// NumSlots is the number of hash slots. Every key maps to exactly one, and
// each slot is owned by one primary node.
const NumSlots = 16384

// crc16tab is the lookup table for CRC16-CCITT (XMODEM): polynomial 0x1021,
// initial value 0. It is built once at startup instead of being pasted in.
var crc16tab = func() (t [256]uint16) {
	for i := range t {
		crc := uint16(i) << 8
		for range 8 {
			if crc&0x8000 != 0 {
				crc = crc<<1 ^ 0x1021
			} else {
				crc <<= 1
			}
		}
		t[i] = crc
	}
	return
}()

// CRC16 computes CRC16-XMODEM, the checksum Redis Cluster uses for keys.
func CRC16(s string) uint16 {
	var crc uint16
	for i := 0; i < len(s); i++ {
		crc = crc<<8 ^ crc16tab[byte(crc>>8)^s[i]]
	}
	return crc
}

// KeyHashSlot returns the slot (0..16383) a key belongs to.
//
// Hash tags: if the key contains "{" followed later by "}" with at least one
// character between them, only the part inside the FIRST such pair is
// hashed. That lets clients force related keys into one slot, e.g.
// "{user1000}.following" and "{user1000}.followers". An empty tag "{}" means
// the whole key is hashed.
func KeyHashSlot(key string) int {
	if open := indexByte(key, '{', 0); open >= 0 {
		if close := indexByte(key, '}', open+1); close > open+1 {
			key = key[open+1 : close]
		}
	}
	// NumSlots is a power of two, so & (NumSlots-1) is the same as % NumSlots.
	return int(CRC16(key) & (NumSlots - 1))
}

func indexByte(s string, c byte, from int) int {
	for i := from; i < len(s); i++ {
		if s[i] == c {
			return i
		}
	}
	return -1
}
