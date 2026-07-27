package hll

import "encoding/binary"

// murmurSeed matches the fixed seed Redis uses in hyperloglog.c so that,
// given the same input bytes, we compute the same (index, rank) pair Redis
// would — which is what actually matters for HLL correctness, independent
// of whether the rest of our byte format matches.
const (
	murmurSeed = 0xadc83b19
	murmurM    = 0xc6a4a7935bd1e995
	murmurR    = 47
)

// murmurHash64A is a direct port of Austin Appleby's MurmurHash64A (the
// 64-bit variant), matching Redis's implementation in src/hyperloglog.c
// byte-for-byte on little-endian input handling.
func murmurHash64A(data []byte) uint64 {
	h := uint64(murmurSeed) ^ (uint64(len(data)) * murmurM)

	nblocks := len(data) / 8
	for i := 0; i < nblocks; i++ {
		k := binary.LittleEndian.Uint64(data[i*8 : i*8+8])
		k *= murmurM
		k ^= k >> murmurR
		k *= murmurM
		h ^= k
		h *= murmurM
	}

	// Tail: same fallthrough structure as the reference C implementation,
	// which folds the remaining 0-7 bytes directly into h (not into a
	// separate k) before the final avalanche.
	tail := data[nblocks*8:]
	switch len(tail) {
	case 7:
		h ^= uint64(tail[6]) << 48
		fallthrough
	case 6:
		h ^= uint64(tail[5]) << 40
		fallthrough
	case 5:
		h ^= uint64(tail[4]) << 32
		fallthrough
	case 4:
		h ^= uint64(tail[3]) << 24
		fallthrough
	case 3:
		h ^= uint64(tail[2]) << 16
		fallthrough
	case 2:
		h ^= uint64(tail[1]) << 8
		fallthrough
	case 1:
		h ^= uint64(tail[0])
		h *= murmurM
	}

	h ^= h >> murmurR
	h *= murmurM
	h ^= h >> murmurR

	return h
}
