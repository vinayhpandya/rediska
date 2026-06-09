package core

import (
	"fmt"
	"strconv"
)

func EncodeArray(args []string) []byte {
	// *3\r\n
	out := []byte(fmt.Sprintf("*%d\r\n", len(args)))
	// $3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n
	for _, arg := range args {
		out = append(out, EncodeBulkString(arg)...)
	}
	return out
}

func EncodeSimpleString(s string) []byte {
	return []byte("+" + s + "\r\n")
}

func EncodeError(s string) []byte {
	return []byte("-" + s + "\r\n")
}

func EncodeBulkString(s string) []byte {
	return []byte("$" + strconv.Itoa(len(s)) + "\r\n" + s + "\r\n")
}

func EncodeNullBulkString() []byte {
	return []byte("$-1\r\n")
}

func EncodeInteger(n int64) []byte {
	return []byte(fmt.Sprintf(":%d\r\n", n))
}
