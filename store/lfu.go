package store

import "math/rand"

// LFU reuses the same 24-bit `entry.lru` field that LRU uses, exactly like
// real Redis reuses its `robj.lru:24` bitfield. The two policies are never
// active on the same key at the same time (maxmemory-policy is global), so
// no extra struct field is needed:
//
//	bits 8-23 (16 bits): ldt  -- last-decay time, in minutes, wraps ~45 days
//	bits 0-7  (8 bits):  counter -- logarithmic access counter, 0-255
const (
	LFUInitVal uint8 = 5 // starting counter for a freshly written key
	// LFULogFactor       = 10 // higher = slower counter growth (config: lfu-log-factor)
	// LFUDecayTime       = 1  // minutes per decay step (config: lfu-decay-time)

	lfuCounterBits = 8
	lfuCounterMask = (1 << lfuCounterBits) - 1 // 0xFF
	lfuLdtMask     = 0xFFFF                    // 16-bit minute clock
)

// LFULogFactor and LFUDecayTime are runtime-tunable (unlike LFUInitVal,
// which real Redis also keeps fixed). They default to Redis's own
// defaults and are overridden via SetLFUParams, called from main.go once
// config flags are parsed.
var (
	LFULogFactor uint32 = 10 // higher = slower counter growth (config: lfu-log-factor)
	LFUDecayTime uint32 = 1  // minutes per decay step (config: lfu-decay-time)
)

func SetLFUParams(logFactor, decayTime int) {
	if logFactor >= 0 {
		LFULogFactor = uint32(logFactor)
	}
	if decayTime >= 1 {
		LFUDecayTime = uint32(decayTime)
	}
}

// lfuDecodeCounter extracts the 8-bit counter from the packed field.
func lfuDecodeCounter(packed uint32) uint8 {
	return uint8(packed & lfuCounterMask)
}

// lfuDecodeLdt extracts the 16-bit last-decay-time (in minutes) from the
// packed field.
func lfuDecodeLdt(packed uint32) uint16 {
	return uint16((packed >> lfuCounterBits) & lfuLdtMask)
}

// lfuEncode packs a counter and ldt back into the shared field.
func lfuEncode(ldt uint16, counter uint8) uint32 {
	return (uint32(ldt) << lfuCounterBits) | uint32(counter)
}

// lfuLogIncr probabilistically increments counter by 1, capped at 255.
// The probability of incrementing shrinks the higher the counter already
// is, which is what lets a single byte usefully represent access counts
// ranging from 1 to many millions.
func lfuLogIncr(counter uint8) uint8 {
	if counter == 255 {
		return counter
	}
	baseVal := float64(counter) - float64(LFUInitVal)
	if baseVal < 0 {
		baseVal = 0
	}
	p := 1.0 / (baseVal*float64(LFULogFactor) + 1)
	if rand.Float64() < p {
		return counter + 1
	}
	return counter
}

// lfuDecay reduces counter based on how many decay steps (LFUDecayTime
// minutes each) have elapsed since it was last touched. idleMinutes should
// be the wraparound-safe difference between "now" and the stored ldt.
func lfuDecay(counter uint8, idleMinutes uint16) uint8 {
	steps := uint32(idleMinutes) / LFUDecayTime
	if steps == 0 {
		return counter
	}
	if uint32(counter) <= steps {
		return 0
	}
	return counter - uint8(steps)
}

// ldtIdle returns the number of minutes elapsed between a stored ldt and
// the current one, handling a single wraparound of the 16-bit minute
// clock the same way lruIdle handles the 24-bit LRU clock.
func ldtIdle(nowLdt, storedLdt uint16) uint16 {
	if nowLdt >= storedLdt {
		return nowLdt - storedLdt
	}
	return nowLdt + (lfuLdtMask - storedLdt)
}

// lfuTouch is the single entry point every access site calls: it decays
// the counter for elapsed idle time, then rolls the probabilistic
// increment, returning a freshly packed field ready to store back on the
// entry. nowLdt is the current 16-bit minute clock.
func lfuTouch(packed uint32, nowLdt uint16) uint32 {
	counter := lfuDecodeCounter(packed)
	storedLdt := lfuDecodeLdt(packed)
	idle := ldtIdle(nowLdt, storedLdt)
	counter = lfuDecay(counter, idle)
	counter = lfuLogIncr(counter)
	return lfuEncode(nowLdt, counter)
}

// lfuNew returns a freshly packed field for a brand-new key: counter
// starts at LFUInitVal (not 0) so a new key survives long enough to be
// accessed again before looking like the coldest thing in the pool.
func lfuNew(nowLdt uint16) uint32 {
	return lfuEncode(nowLdt, LFUInitVal)
}

// lfuClock returns the current 16-bit minute clock, derived from the
// injectable now() so tests stay deterministic (mirrors Store.lruClock).
func (s *Store) lfuClock() uint16 {
	return uint16((s.now().Unix() / 60) & lfuLdtMask)
}
