// Package core implements RESP, the protocol Redis clients and servers
// speak, and the append-only file (AOF) used for persistence.
//
// The Encode functions build replies. Each returns the complete bytes of
// one RESP value, ready to write to a connection. The Decode functions read
// values sent by clients, and AOF stores write commands in the same format.
package core

import (
	"fmt"
	"strconv"
)

// EncodeArray encodes args as a RESP array of bulk strings, the format
// clients use to send commands: SET k v becomes
// "*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n".
// To build an array of other kinds of values, use EncodeRawArray.
func EncodeArray(args []string) []byte {
	// *3\r\n
	out := []byte(fmt.Sprintf("*%d\r\n", len(args)))
	// $3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n
	for _, arg := range args {
		out = append(out, EncodeBulkString(arg)...)
	}
	return out
}

// EncodeSimpleString encodes s as a RESP simple string, such as "+OK\r\n".
// s must not contain \r or \n; use EncodeBulkString for arbitrary text.
func EncodeSimpleString(s string) []byte {
	return []byte("+" + s + "\r\n")
}

// EncodeError encodes s as a RESP error reply, such as
// "-ERR unknown command\r\n". By convention s starts with an error code in
// capitals (ERR, MOVED, CROSSSLOT, ...), which clients use to tell errors
// apart.
func EncodeError(s string) []byte {
	return []byte("-" + s + "\r\n")
}

// EncodeRawArray wraps items, each already RESP-encoded, in a RESP array.
// Because an encoded array is itself an item, calls can be nested to build
// arrays of arrays, as CLUSTER SLOTS does. A nil or empty items gives the
// empty array "*0\r\n".
func EncodeRawArray(items [][]byte) []byte {
	out := []byte(fmt.Sprintf("*%d\r\n", len(items)))
	for _, item := range items {
		out = append(out, item...)
	}
	return out
}

// EncodeBulkString encodes s as a RESP bulk string, such as
// "$5\r\nvinay\r\n". Bulk strings may contain any bytes, including
// newlines.
func EncodeBulkString(s string) []byte {
	return []byte("$" + strconv.Itoa(len(s)) + "\r\n" + s + "\r\n")
}

// EncodeNullBulkString returns RESP's nil reply, "$-1\r\n", which means
// "no value", for example GET on a key that doesn't exist.
func EncodeNullBulkString() []byte {
	return []byte("$-1\r\n")
}

// EncodeInteger encodes n as a RESP integer, such as ":42\r\n".
func EncodeInteger(n int64) []byte {
	return []byte(fmt.Sprintf(":%d\r\n", n))
}
