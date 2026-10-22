package store

// Encoding says how a value is stored in memory. OBJECT ENCODING reports
// it, using the same names as Redis.
type Encoding int

const (
	// EncodingINT stores a string that is a valid 64-bit integer as a number.
	EncodingINT Encoding = iota
	// EncodingEMBSTR is a short string (44 bytes or less).
	EncodingEMBSTR
	// EncodingRAW is a longer string, or any string that has been changed
	// by APPEND.
	EncodingRAW
	// EncodingHLL is a HyperLogLog, created by PFADD or PFMERGE.
	EncodingHLL
)
