package hll

// This file implements the three sparse opcodes we worked through earlier:
//
//   ZERO:   00xxxxxx            1 byte  -- run of 1-64 zero registers
//   XZERO:  01xxxxxx yyyyyyyy   2 bytes -- run of 1-16384 zero registers
//   VAL:    1vvvvvll            1 byte  -- run of 1-4 registers, same value
//
// Each opcode's *type* is identified purely by its leading bits, which is
// why decoding always looks at the first byte before deciding how many
// bytes to consume.

const (
	hllSparseMaxBytes = 3000 // promote to dense once the sparse form exceeds this

	sparseValMaxValue = 32 // VAL's 5 value-bits store (value-1), so max value = 32
	sparseValMaxLen   = 4  // VAL's 2 length-bits store (len-1), so max run = 4

	sparseZeroMaxLen  = 64    // ZERO's 6 length-bits store (len-1), so max run = 64
	sparseXZeroMaxLen = 16384 // XZERO's 14 length-bits store (len-1), so max run = 16384
)

// encodeZero returns the 1-byte ZERO opcode for a run of `length` zero
// registers. Caller must ensure 1 <= length <= 64.
func encodeZero(length int) byte {
	// length-1 because 6 bits can only count 0..63, but we want to
	// represent runs of 1..64 -- shifting the representable range up by
	// one via the same "-1" trick VAL and XZERO also use.
	return byte(length - 1)
	// Top 2 bits are implicitly 00 here because length-1 maxes out at 63
	// (0b00111111), which never sets either of the top two bits.
}

// encodeXZero returns the 2-byte XZERO opcode for a run of `length` zero
// registers. Caller must ensure 1 <= length <= 16384.
func encodeXZero(length int) [2]byte {
	n := uint16(length - 1) // same -1 trick, now over 14 bits (0..16383)
	first := byte(0x40 | (n >> 8))
	// 0x40 is binary 01000000 -- this hard-codes the "01" opcode marker
	// into the top 2 bits. n>>8 pulls out the top 6 bits of our 14-bit
	// number (everything above bit 7), which fit in this byte's
	// remaining 6 bits. OR-ing combines "01" + those 6 bits into one byte.
	second := byte(n) // truncating uint16->byte keeps only the low 8 bits,
	// which is exactly the bottom 8 bits of our 14-bit number -- the
	// second byte carries no opcode marker, it's pure data.
	return [2]byte{first, second}
}

// encodeVal returns the 1-byte VAL opcode for `length` consecutive
// registers all holding `value`. Caller must ensure 1 <= value <= 32 and
// 1 <= length <= 4.
func encodeVal(value uint8, length int) byte {
	v := value - 1        // -1 trick again: 5 bits store 0..31, representing 1..32
	l := byte(length - 1) // 2 bits store 0..3, representing 1..4
	return 0x80 | (v << 2) | l
	// 0x80 is binary 10000000 -- hard-codes the leading "1" opcode marker.
	// v<<2 shifts our 5 value-bits left by 2, making room below them for
	// the 2 length-bits. OR-ing all three pieces packs them into one byte:
	// [1][vvvvv][ll].
}

// sparseOpKind identifies which of the three opcodes a byte represents.
type sparseOpKind int

const (
	opZero sparseOpKind = iota
	opXZero
	opVal
)

// decodeOpKind inspects just the first byte of an opcode (without
// consuming anything) to determine which of the three types it is, which
// tells the caller how many total bytes to read next.
func decodeOpKind(b byte) sparseOpKind {
	if b&0x80 != 0 {
		// Top bit set -- matches VAL's leading "1", regardless of what
		// the second-from-top bit is (that bit is part of VAL's value
		// field, not a fixed marker, so we must check bit 7 alone here).
		return opVal
	}
	if b&0x40 != 0 {
		// Top bit is 0 (else we'd have returned above) and this bit
		// (bit 6) is 1 -- together that's exactly "01", the XZERO marker.
		return opXZero
	}
	// Neither bit 7 nor bit 6 set -- top two bits are "00", the ZERO marker.
	return opZero
}

// decodeZero extracts the run length from a ZERO opcode byte.
func decodeZero(b byte) int {
	return int(b) + 1 // undo the -1 trick from encodeZero
	// Note: b's top 2 bits are guaranteed 0 here (caller already checked
	// decodeOpKind == opZero), so int(b) alone is already just the raw
	// 6-bit count -- no masking needed.
}

// decodeXZero extracts the run length from a 2-byte XZERO opcode.
func decodeXZero(b0, b1 byte) int {
	n := (uint16(b0&0x3F) << 8) | uint16(b1)
	// b0&0x3F masks off the top 2 marker bits (0x3F = 00111111), leaving
	// only the 6 data bits of the first byte. Shifting left 8 makes room
	// below them for all 8 bits of the second byte, then OR-ing merges
	// the two into one 14-bit number: [6 bits from b0][8 bits from b1].
	return int(n) + 1 // undo the -1 trick
}

// decodeVal extracts the value and run length from a VAL opcode byte.
func decodeVal(b byte) (value uint8, length int) {
	value = ((b >> 2) & 0x1F) + 1
	// b>>2 shifts the length bits off the bottom, leaving [1][vvvvv] in
	// the low 6 bits. &0x1F (0b00011111) then masks off the leftover
	// leading "1" marker bit, isolating just the 5 value bits. +1 undoes
	// the -1 trick.
	length = int(b&0x03) + 1
	// b&0x03 (0b00000011) keeps only the bottom 2 bits -- the length
	// field -- discarding everything else. +1 undoes the -1 trick.
	return
}

// walkSparse decodes the opcode stream in `sparse` from front to back,
// calling visit once per opcode with:
//
//	startIndex - the register index this run begins at
//	runLen     - how many consecutive registers this run covers
//	value      - the register value for this run (0 for ZERO/XZERO runs)
//
// visit returns false to stop walking early (used by sparseGet, which only
// needs to find the one run containing a specific index and doesn't care
// about anything after it), or true to keep going.
func walkSparse(sparse []byte, visit func(startIndex uint64, runLen int, value uint8) bool) {
	var idx uint64 // tracks "which register are we currently at" as we
	// move through the stream -- this has to be computed by summing up
	// every prior run's length, since (unlike dense) nothing in the byte
	// stream itself says "this opcode is for register 5000."

	pos := 0 // byte offset into `sparse` -- how far we've read so far
	for pos < len(sparse) {
		b := sparse[pos]
		switch decodeOpKind(b) {
		case opZero:
			length := decodeZero(b)
			if !visit(idx, length, 0) {
				return
			}
			idx += uint64(length)
			pos++ // ZERO is always exactly 1 byte
		case opXZero:
			length := decodeXZero(b, sparse[pos+1])
			if !visit(idx, length, 0) {
				return
			}
			idx += uint64(length)
			pos += 2 // XZERO is always exactly 2 bytes
		case opVal:
			value, length := decodeVal(b)
			if !visit(idx, length, value) {
				return
			}
			idx += uint64(length)
			pos++ // VAL is always exactly 1 byte
		}
	}
}

// sparseGet returns the value of register `index` by walking the opcode
// stream until it finds the run that contains it.
func sparseGet(sparse []byte, index uint64) uint8 {
	var result uint8
	walkSparse(sparse, func(startIndex uint64, runLen int, value uint8) bool {
		if index >= startIndex && index < startIndex+uint64(runLen) {
			result = value
			return false
		}
		return true
	})
	return result
}

// sparseHisto builds a full register-value histogram by walking every
// opcode -- the sparse equivalent of looping over a dense array and
// counting each register's value.
func sparseHisto(sparse []byte) [64]int {
	var histo [64]int
	walkSparse(sparse, func(startIndex uint64, runLen int, value uint8) bool {
		histo[value] += runLen
		return true
	})
	return histo
}

// encodeZeroRun picks ZERO (1 byte, max 64) or XZERO (2 bytes, max
// 16384) depending on how long a zero-run needs to be, so sparseSet
// doesn't have to make that choice inline.
func encodeZeroRun(length int) []byte {
	if length <= sparseZeroMaxLen {
		return []byte{encodeZero(length)}
	}
	xz := encodeXZero(length)
	return xz[:]
}

// sparseSet returns a new opcode stream with register `index` updated to
// `val`, IF val is larger than what's currently stored there (same
// grow-only rule as dense). Returns the original slice unchanged and
// `false` if no update was needed.
//
// Unlike sparseGet/sparseHisto, this can't be built on top of walkSparse:
// splicing new opcodes into the byte stream requires knowing exact BYTE
// offsets (not just register offsets), which walkSparse's callback
// doesn't expose. So this re-implements the same walk loop by hand,
// tracking byte position explicitly alongside register position.
func sparseSet(sparse []byte, index uint64, val uint8) ([]byte, bool) {
	idx := uint64(0)
	pos := 0

	for pos < len(sparse) {
		b := sparse[pos]
		kind := decodeOpKind(b)

		var runLen int
		var value uint8
		var opBytes int

		switch kind {
		case opZero:
			runLen = decodeZero(b)
			opBytes = 1
		case opXZero:
			runLen = decodeXZero(b, sparse[pos+1])
			opBytes = 2
		case opVal:
			value, runLen = decodeVal(b)
			opBytes = 1
		}

		// Is the register we're looking for inside THIS opcode's range?
		if index >= idx && index < idx+uint64(runLen) {
			if value >= val {
				return sparse, false // already >= val, nothing to do
			}

			before := int(index - idx)   // registers before index, in this run
			after := runLen - before - 1 // registers after index, in this run

			var replacement []byte
			if kind == opVal {
				// Splitting a VAL run: before/after keep the OLD value.
				// VAL's max original runLen is 4, so before/after are
				// at most 3 -- always fits in a single VAL opcode each,
				// no chunking needed.
				if before > 0 {
					replacement = append(replacement, encodeVal(value, before))
				}
				replacement = append(replacement, encodeVal(val, 1))
				if after > 0 {
					replacement = append(replacement, encodeVal(value, after))
				}
			} else {
				// Splitting a ZERO/XZERO run: before/after are still zero.
				if before > 0 {
					replacement = append(replacement, encodeZeroRun(before)...)
				}
				replacement = append(replacement, encodeVal(val, 1))
				if after > 0 {
					replacement = append(replacement, encodeZeroRun(after)...)
				}
			}

			// Splice: everything before this opcode + replacement +
			// everything after this opcode. This is the "before/VAL/
			// after" split we walked through by hand earlier.
			out := make([]byte, 0, len(sparse)-opBytes+len(replacement))
			out = append(out, sparse[:pos]...)
			out = append(out, replacement...)
			out = append(out, sparse[pos+opBytes:]...)
			return out, true
		}

		idx += uint64(runLen)
		pos += opBytes
	}

	return sparse, false // index out of range -- shouldn't happen for valid input
}
