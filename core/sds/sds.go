package sds

const sdsMaxPreAlloc = 1024 * 1024

type SDS struct {
	buf []byte
}

func New(s string) *SDS {
	b := make([]byte, len(s))
	copy(b, s)
	return &SDS{buf: b}
}

func (s *SDS) Len() int {
	return len(s.buf)
}

func (s *SDS) Cap() int {
	return cap(s.buf)
}

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
