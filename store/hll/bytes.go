package hll

import "fmt"

// Bytes returns h as bytes that FromBytes turns back into an equal HLL:
// one byte for the encoding (0 sparse, 1 dense), then the registers in
// that encoding. Replication uses it to copy a HyperLogLog to a replica.
func (h *HLL) Bytes() []byte {
	data := h.sparse
	if h.enc == encDense {
		data = h.dense
	}
	out := make([]byte, 0, 1+len(data))
	out = append(out, byte(h.enc))
	return append(out, data...)
}

// FromBytes rebuilds an HLL from what Bytes returned. It rejects anything
// that isn't the right shape, rather than build a broken sketch.
func FromBytes(b []byte) (*HLL, error) {
	if len(b) < 2 {
		return nil, fmt.Errorf("hll: %d bytes is too short", len(b))
	}
	data := append([]byte(nil), b[1:]...) // a copy: the caller may reuse b
	switch encMode(b[0]) {
	case encSparse:
		return &HLL{enc: encSparse, sparse: data, dirty: true}, nil
	case encDense:
		if len(data) != len(newDenseArray()) {
			return nil, fmt.Errorf("hll: dense registers are %d bytes, want %d", len(data), len(newDenseArray()))
		}
		return &HLL{enc: encDense, dense: data, dirty: true}, nil
	default:
		return nil, fmt.Errorf("hll: unknown encoding %d", b[0])
	}
}
