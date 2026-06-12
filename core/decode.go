package core

import (
	"strconv"
	"strings"
)

func DecodeInteger(b []byte) (int64, error) {
	s := strings.TrimSuffix(strings.TrimPrefix(string(b), ":"), "\r\n")
	return strconv.ParseInt(s, 10, 64)
}
