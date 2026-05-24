package core

import (
	"fmt"
	"strconv"
)

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
