// Package hll implements a HyperLogLog with both sparse and dense
// encodings, modeled on Redis's HLL_SPARSE / HLL_DENSE representations
// (src/hyperloglog.c). See hll_sparse.go for the opcode format and the
// sparse-specific read/write logic.
package hll

import (
	"log"
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

// encMode says which of HLL's two register representations is currently
// live. Only ever moves sparse -> dense, never back (matches Redis).
type encMode uint8

const (
	encSparse encMode = iota
	encDense
)

// HLL is a HyperLogLog sketch. Exactly one of dense/sparse is populated
// at a time, selected by enc.
type HLL struct {
	dense  []byte // valid when enc == encDense: packed 6-bit counters
	sparse []byte // valid when enc == encSparse: opcode stream

	enc encMode

	// cached is the last computed cardinality estimate, reused by Count
	// as long as dirty is false. See Add/Merge for where dirty gets set.
	cached uint64
	dirty  bool
}

// New creates an empty HLL, starting in sparse form: a single XZERO
// opcode covering all 16384 registers, exactly like a fresh Redis HLL.
func New() *HLL {
	xz := encodeXZero(registers)
	return &HLL{
		enc:    encSparse,
		sparse: xz[:], // convert the fixed-size [2]byte array to a slice
	}
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

func (h *HLL) Size() int {
	if h.enc == encSparse {
		return len(h.sparse)
	}
	return len(h.dense)
}

// Encoding reports which representation is currently active ("sparse" or
// "dense") -- mirrors Redis's PFDEBUG ENCODING, mainly useful for tests
// and debugging.
func (h *HLL) Encoding() string {
	if h.enc == encSparse {
		return "sparse"
	}
	return "dense"
}

// -- dense register access -------------------------------------------
//
// These are free functions (not methods) rather than tied to *HLL, so
// that promote() can build and fill a brand-new dense array before it's
// ever attached to an HLL struct, and so Merge can read directly from
// another HLL's dense array without extra plumbing.

// getDenseRegister reads the 6-bit counter at regnum, spanning at most
// two bytes.
func getDenseRegister(dense []byte, regnum uint64) uint8 {
	byteIdx := regnum * hllBits / 8
	firstBit := regnum * hllBits % 8
	remBits := 8 - firstBit

	b0 := uint16(dense[byteIdx])
	b1 := uint16(dense[byteIdx+1])

	return uint8((b0>>firstBit | b1<<remBits) & registerMax)
}

// setDenseRegister writes val into the counter at regnum, but only if
// val is larger than what's already stored (HLL registers only ever
// grow). Returns true if the register actually changed.
func setDenseRegister(dense []byte, regnum uint64, val uint8) bool {
	if val > registerMax {
		val = registerMax // clamp; in practice never hit at P=14
	}
	if val <= getDenseRegister(dense, regnum) {
		return false
	}

	byteIdx := regnum * hllBits / 8
	firstBit := regnum * hllBits % 8
	remBits := 8 - firstBit

	dense[byteIdx] &^= registerMax << firstBit
	dense[byteIdx] |= val << firstBit

	dense[byteIdx+1] &^= registerMax >> remBits
	dense[byteIdx+1] |= val >> remBits

	return true
}

// newDenseArray allocates a zeroed dense register array: one 6-bit slot
// per register, plus one padding byte so the last register's high bits
// never read/write out of bounds.
func newDenseArray() []byte {
	size := (registers*hllBits+7)/8 + 1
	return make([]byte, size)
}

// promote converts h from sparse to dense, permanently -- matches Redis:
// once dense, an HLL never converts back to sparse, even though nothing
// here would technically prevent it.
func (h *HLL) promote() {
	dense := newDenseArray()

	// Walk every sparse run and copy its value into the new dense array.
	// Zero runs need no work at all, since newDenseArray already starts
	// fully zeroed -- only non-zero VAL runs need copying.
	log.Printf("hll: promoting sparse -> dense (sparse was %d bytes, dense is %d bytes)",
		len(h.sparse), len(dense))
	walkSparse(h.sparse, func(startIndex uint64, runLen int, value uint8) bool {
		if value != 0 {
			for i := 0; i < runLen; i++ {
				setDenseRegister(dense, startIndex+uint64(i), value)
			}
		}
		return true
	})

	h.dense = dense
	h.sparse = nil
	h.enc = encDense
}

// Add hashes data and updates the relevant register if this element
// produced a longer run of leading zeros than previously seen there.
// Returns true if the register (and therefore the estimate) changed.
func (h *HLL) Add(data []byte) bool {
	index, rank := hllPatLen(data)

	// A rank this high can't be encoded as a sparse VAL opcode (max 32
	// per the -1 trick over 5 bits) -- vanishingly rare at P=14, but
	// promoting first keeps sparseSet's assumptions valid.
	if h.enc == encSparse && rank > sparseValMaxValue {
		h.promote()
	}

	var changed bool
	switch h.enc {
	case encSparse:
		newSparse, ch := sparseSet(h.sparse, index, rank)
		if ch {
			h.sparse = newSparse
			changed = true
		}
		// Grew past the point where sparse is still a space win --
		// promote once, permanently.
		if len(h.sparse) > hllSparseMaxBytes {
			h.promote()
		}
	case encDense:
		changed = setDenseRegister(h.dense, index, rank)
	}

	if changed {
		h.dirty = true
	}
	return changed
}

// Merge folds other's registers into h via an elementwise max -- this is
// the same operation Redis's PFMERGE (and multi-key PFCOUNT, on a
// scratch copy) performs. The result always ends up dense (h is
// promoted first if it was sparse), matching how Redis computes merges
// internally rather than trying to splice two sparse streams together.
// Returns true if any register in h changed.
func (h *HLL) Merge(other *HLL) bool {
	if h.enc == encSparse {
		h.promote()
	}

	changed := false
	switch other.enc {
	case encDense:
		for i := uint64(0); i < registers; i++ {
			if setDenseRegister(h.dense, i, getDenseRegister(other.dense, i)) {
				changed = true
			}
		}
	case encSparse:
		walkSparse(other.sparse, func(startIndex uint64, runLen int, value uint8) bool {
			if value != 0 {
				for i := 0; i < runLen; i++ {
					if setDenseRegister(h.dense, startIndex+uint64(i), value) {
						changed = true
					}
				}
			}
			return true
		})
	}

	if changed {
		h.dirty = true
	}
	return changed
}

// Count returns the estimated cardinality, using the harmonic-mean
// estimator with the standard low-cardinality linear-counting
// correction. (Large-range bias correction is omitted here since a
// 64-bit hash never approaches the collision range that requires it.)
//
// If no register has changed since the last call, the cached result is
// returned directly without rescanning -- this is the common case, since
// most Add calls don't actually beat an existing register's max value.
func (h *HLL) Count() uint64 {
	if !h.dirty {
		return h.cached
	}

	var histo [64]int // histo[v] = number of registers holding value v
	switch h.enc {
	case encSparse:
		histo = sparseHisto(h.sparse)
	case encDense:
		for i := uint64(0); i < registers; i++ {
			histo[getDenseRegister(h.dense, i)]++
		}
	}

	m := float64(registers)
	alpha := alphaInf // valid for m>=128, which covers our P=14 case exactly.

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
