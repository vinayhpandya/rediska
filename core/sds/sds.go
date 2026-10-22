// Package sds implements a growable byte string, modelled on Redis's
// simple dynamic strings (SDS). Rediska stores string values in an SDS so
// that APPEND can usually grow a value in place instead of copying it.
package sds

const sdsMaxPreAlloc = 1024 * 1024

// SDS is a growable string. Create one with New.
type SDS struct {
	buf []byte
}

// New returns an SDS holding a copy of s.
func New(s string) *SDS {
	b := make([]byte, len(s))
	copy(b, s)
	return &SDS{buf: b}
}

// Len returns the length of the string in bytes.
func (s *SDS) Len() int {
	return len(s.buf)
}

// Cap returns how many bytes the string can hold before it must grow.
func (s *SDS) Cap() int {
	return cap(s.buf)
}

// String returns the contents as a Go string (a copy).
func (s *SDS) String() string {
	return string(s.buf)
}

func (s *SDS) append(bytes []byte) {
	needed := len(s.buf) + len(bytes)
	if needed > sdsMaxPreAlloc {
		s.growTo(needed)
	}
	s.buf = append(s.buf, bytes...)
}

// AppendString adds v to the end of the string. Once the string grows past
// 1 MB, extra room is reserved in 1 MB steps, as Redis does, so repeated
// appends don't copy the whole value each time.
func (s *SDS) AppendString(v string) {
	s.append([]byte(v))
}

func (s *SDS) growTo(needed int) {
	var newCap int
	if needed < sdsMaxPreAlloc {
		newCap = needed * 2
	} else {
		newCap = needed + sdsMaxPreAlloc
	}
	newBuf := make([]byte, len(s.buf), newCap)
	copy(newBuf, s.buf)
	s.buf = newBuf
}
