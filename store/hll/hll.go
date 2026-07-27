package hll

// Package hll implements a dense-only HyperLogLog, modeled on Redis's
// HLL_DENSE representation (src/hyperloglog.c).
//
// Sparse encoding, promotion, and Merge are intentionally left out for now —
// this is step 1 of a staged implementation.

import (
	"math"
	"math/bits"
)

const (
	// P controls precision: 2^P registers. Redis uses P=14 -> 16384 registers,
	// giving a standard error of ~0.81%.
	p = 14

	// registers is the number of 6-bit counters (m in the estimator formula).
	registers = 1 << p

	// q is the number of hash bits left over after using P bits for the
	// register index. The longest possible run of leading zeros is q+1.
	q = 64 - p

	// hllBits is the width of each register counter. 6 bits can count runs
	// up to 63, more than enough since max run is q+1 = 51 for P=14.
	hllBits = 6

	registerMax = (1 << hllBits) - 1 // 63

	// alphaInf is the bias-correction constant used in the estimator,
	// 0.5/ln(2), matching HLL_ALPHA_INF in the Redis source.
	alphaInf = 0.721347520444481703680
)

// MurmurHash64A is assumed to already exist elsewhere in this package
// (or an equivalent 64-bit MurmurHash2 implementation). Signature shown
// here only so this file compiles standalone as a reference:
//
//	func MurmurHash64A(data []byte, seed uint32) uint64

// HLL is a dense HyperLogLog sketch.
type HLL struct {
	// registers holds ceil(registers*hllBits/8) bytes, packing 6-bit
	// counters back to back (LSB to MSB), plus one extra byte of padding
	// so the final counter's high bits never read/write out of bounds.
	regs []byte

	// cached is the last computed cardinality estimate.
	cached uint64
	// dirty is true if a register has changed since cached was computed,
	// meaning Count must recompute rather than reuse cached. Mirrors the
	// "cached cardinality valid" bit in Redis's HLL header.
	dirty bool
}

// New creates an empty dense HLL.
func New() *HLL {
	size := (registers*hllBits+7)/8 + 1 // +1 padding byte, mirrors Redis's
	// reliance on sds's implicit null terminator for the same trick.
	return &HLL{regs: make([]byte, size)}
	// Note: dirty defaults to false and cached to 0, which is correct —
	// an empty HLL's true cardinality is 0, so there's nothing to
	// recompute until the first register actually changes.
}

func (h *HLL) Merge(other *HLL) (changed bool) {
	for i := uint64(0); i < registers; i++ {
		if h.denseSet(i, other.denseGet(i)) {
			changed = true
		}
	}
	if changed {
		h.dirty = true
	}
	return changed
}

// hllPatLen hashes data, returning the register index it maps to and the
// "rank": the position of the first 1-bit (from the LSB) in the remaining
// hash bits, i.e. length of the leading run of zeros + 1.
func hllPatLen(data []byte) (index uint64, rank uint8) {
	hash := murmurHash64A(data)

	index = hash & (registers - 1) // low P bits select the register
	hash >>= p                     // remaining bits determine the rank

	// Force-set bit q so TrailingZeros64 is bounded even if the upper
	// bits are all zero (guarantees rank <= q+1, mirrors the Redis
	// "hash |= (1<<HLL_Q)" trick).
	hash |= uint64(1) << q

	rank = uint8(bits.TrailingZeros64(hash)) + 1
	return
}

// denseGet reads the 6-bit counter at regnum, spanning at most two bytes.
func (h *HLL) denseGet(regnum uint64) uint8 {
	byteIdx := regnum * hllBits / 8
	firstBit := regnum * hllBits % 8
	remBits := 8 - firstBit

	b0 := uint16(h.regs[byteIdx])
	b1 := uint16(h.regs[byteIdx+1])

	return uint8((b0>>firstBit | b1<<remBits) & registerMax)
}

// denseSet writes val into the counter at regnum, but only if val is
// larger than what's already stored (HLL registers only ever grow).
// Returns true if the register actually changed.
func (h *HLL) denseSet(regnum uint64, val uint8) bool {
	if val > registerMax {
		val = registerMax // clamp; in practice never hit at P=14
	}
	if val <= h.denseGet(regnum) {
		return false
	}

	byteIdx := regnum * hllBits / 8
	firstBit := regnum * hllBits % 8
	remBits := 8 - firstBit

	h.regs[byteIdx] &^= registerMax << firstBit
	h.regs[byteIdx] |= val << firstBit

	h.regs[byteIdx+1] &^= registerMax >> remBits
	h.regs[byteIdx+1] |= val >> remBits

	return true
}

// Add hashes data and updates the relevant register if this element
// produced a longer run of leading zeros than previously seen there.
// Returns true if the register (and therefore the estimate) changed.
func (h *HLL) Add(data []byte) bool {
	index, rank := hllPatLen(data)
	changed := h.denseSet(index, rank)
	if changed {
		h.dirty = true
	}
	return changed
}

// Count returns the estimated cardinality, using the harmonic-mean
// estimator with Redis/original-paper's low-cardinality linear-counting
// correction. (Large-range bias correction is omitted here since a 64-bit
// hash never approaches the collision range that requires it.)
//
// If no register has changed since the last call, the cached result is
// returned directly without rescanning all 16384 registers — this is the
// common case, since most Add calls don't actually beat an existing
// register's max value.
func (h *HLL) Count() uint64 {
	if !h.dirty {
		return h.cached
	}

	var histo [64]int // histo[v] = number of registers holding value v
	for i := uint64(0); i < registers; i++ {
		histo[h.denseGet(i)]++
	}

	m := float64(registers)
	alpha := alphaInf // for m=16384 this is very close to alphaInf already;
	// Redis actually uses a fixed table for small m and alphaInf for
	// m>=128, which covers our P=14 case exactly.

	sum := 0.0
	for val, count := range histo {
		sum += float64(count) / math.Pow(2, float64(val))
	}

	estimate := alpha * m * m / sum

	// Low-cardinality correction: if the estimate is small relative to m,
	// fall back to linear counting, which is much more accurate when many
	// registers are still zero.
	if estimate <= 2.5*m {
		zeros := histo[0]
		if zeros != 0 {
			estimate = m * math.Log(m/float64(zeros))
		}
	}

	h.cached = uint64(estimate + 0.5)
	h.dirty = false
	return h.cached
}
