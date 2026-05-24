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

func readBulkString(r *bufio.Reader) (string, error) {
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
	buffer := make([]byte, length)
	if _, err := io.ReadFull(r, buffer); err != nil {
		return "", fmt.Errorf("reading bulk string body: %w", err)
	}
	cr, err := r.ReadByte()
	if err != nil {
		return "", err
	}
	lf, err := r.ReadByte()
	if err != nil {
		return "", err
	}
	if cr != '\r' || lf != '\n' {
		return "", fmt.Errorf("bulk string not terminated with \\r\\n, got %q%q", cr, lf)
	}
	return string(buffer), nil
}

func readArray(r *bufio.Reader) ([]any, error) {
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
		val, err := Decode(r)
		if err != nil {
			return nil, fmt.Errorf("Error reading array element %d, %w", i, err)
		}
		result[i] = val
	}
	return result, nil
}

func Decode(r *bufio.Reader) (any, error) {
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
		return readBulkString(r)
	case '*':
		return readArray(r)
	default:
		return nil, fmt.Errorf("unknown RESP type byte: %q", typeByte)
	}
}
