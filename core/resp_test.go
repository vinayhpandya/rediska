package core

import (
	"bufio"
	"reflect"
	"strings"
	"testing"
)

func TestDecode(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  any
	}{
		{"simple string", "+OK\r\n", "OK"},
		{"error", "-ERR boom\r\n", "ERR boom"},
		{"integer", ":42\r\n", int64(42)},
		{"bulk string", "$5\r\nhello\r\n", "hello"},
		{"null bulk", "$-1\r\n", ""},
		{"PING command", "*1\r\n$4\r\nPING\r\n", []any{"PING"}},
		{"SET foo bar", "*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n", []any{"SET", "foo", "bar"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := bufio.NewReader(strings.NewReader(tt.input))
			got, err := Decode(r)
			if err != nil {
				t.Fatalf("Decode() error = %v", err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Decode() = %v (%T), want %v (%T)", got, got, tt.want, tt.want)
			}
		})
	}
}
