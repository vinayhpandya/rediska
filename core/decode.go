package core

import (
	"strconv"
	"strings"
)

// DecodeInteger parses an encoded RESP integer reply, such as ":42\r\n",
// back into a number. The server uses it to read replies it produced
// itself, such as the active-expiry loop's count of deleted keys.
func DecodeInteger(b []byte) (int64, error) {
	s := strings.TrimSuffix(strings.TrimPrefix(string(b), ":"), "\r\n")
	return strconv.ParseInt(s, 10, 64)
}
