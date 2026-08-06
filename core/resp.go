package core

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
)

func readLine(r *bufio.Reader) (string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return "", err
	}
	if len(line) < 2 || line[len(line)-2] != '\r' {
		return "", fmt.Errorf("malformed line, expected \\r\\n terminator: %q", line)
	}
	return line[:len(line)-2], nil
}

func readSimpleString(r *bufio.Reader) (string, error) {
	return readLine(r)
}

func readError(r *bufio.Reader) (string, error) {
	return readLine(r)
}

func readInteger(r *bufio.Reader) (int64, error) {
	line, err := readLine(r)
	if err != nil {
		return 0, err
	}
	n, err := strconv.ParseInt(line, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid integer %q: %w", line, err)
	}
	return n, nil
}

func readBulkString(r *bufio.Reader, scratch *[]byte) (string, error) {
	line, err := readLine(r)
	if err != nil {
		return "", err
	}
	length, err := strconv.Atoi(line)
	if err != nil {
		return "", fmt.Errorf("invalid bulk string length %q: %w", line, err)
	}
	if length == -1 {
		return "", nil
	}
	// Read the body and its trailing \r\n in a single ReadFull instead of
	// io.ReadFull + two separate ReadByte calls.
	need := length + 2
	var buffer []byte
	if scratch != nil {
		// Reuse the caller's scratch buffer across calls (grows as needed)
		// instead of allocating a fresh []byte for every argument of every
		// command. Only used on hot paths that decode many commands in a
		// row from the same connection; nil scratch preserves the old
		// one-shot-allocation behavior for callers like AOF replay.
		if cap(*scratch) < need {
			*scratch = make([]byte, need)
		}
		buffer = (*scratch)[:need]
	} else {
		buffer = make([]byte, need)
	}
	if _, err := io.ReadFull(r, buffer); err != nil {
		return "", fmt.Errorf("reading bulk string body: %w", err)
	}
	if buffer[length] != '\r' || buffer[length+1] != '\n' {
		return "", fmt.Errorf("bulk string not terminated with \\r\\n, got %q%q", buffer[length], buffer[length+1])
	}
	// string(buffer[:length]) always copies, so this is safe even though
	// buffer may be reused (via scratch) on the next call.
	return string(buffer[:length]), nil
}

func readArray(r *bufio.Reader, scratch *[]byte) ([]any, error) {
	line, err := readLine(r)
	if err != nil {
		return nil, err
	}
	count, err := strconv.Atoi(line)
	if err != nil {
		return nil, fmt.Errorf("invalid array length %q: %w", line, err)
	}
	result := make([]any, count)
	for i := 0; i < count; i++ {
		val, err := decode(r, scratch)
		if err != nil {
			return nil, fmt.Errorf("Error reading array element %d, %w", i, err)
		}
		result[i] = val
	}
	return result, nil
}

func decode(r *bufio.Reader, scratch *[]byte) (any, error) {
	typeByte, err := r.ReadByte()
	if err != nil {
		return nil, err
	}
	switch typeByte {
	case '+':
		return readSimpleString(r)
	case '-':
		return readError(r)
	case ':':
		return readInteger(r)
	case '$':
		return readBulkString(r, scratch)
	case '*':
		return readArray(r, scratch)
	default:
		return nil, fmt.Errorf("unknown RESP type byte: %q", typeByte)
	}
}

// Decode parses one RESP value from r. Each call that reaches a bulk string
// allocates its own buffer — fine for occasional/one-shot use (e.g. AOF
// replay, tests). For decoding many commands in a row from the same
// connection, use NewDecoder instead to reuse a scratch buffer across calls.
func Decode(r *bufio.Reader) (any, error) {
	return decode(r, nil)
}

// Decoder wraps a *bufio.Reader with a reusable scratch buffer, so decoding
// many commands in a row (the normal connection hot path) doesn't allocate a
// fresh []byte for every bulk-string argument of every command.
type Decoder struct {
	r       *bufio.Reader
	scratch []byte
}

func NewDecoder(r *bufio.Reader) *Decoder {
	return &Decoder{r: r}
}

// Decode parses one RESP value, reusing this Decoder's scratch buffer for
// any bulk-string bodies encountered.
func (d *Decoder) Decode() (any, error) {
	return decode(d.r, &d.scratch)
}
