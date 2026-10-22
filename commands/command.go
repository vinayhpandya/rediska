package commands

import (
	"fmt"
	"strings"
)

// Command is one parsed client request, such as SET name vinay.
type Command struct {
	Name string // uppercased for case-insensitive lookup
	Args []string
}

// ParseCommand turns a value decoded by core.Decode into a Command. The
// value must be a non-empty array of bulk strings; the first one becomes
// the upper-cased command name.
func ParseCommand(decoded any) (*Command, error) {
	arr, ok := decoded.([]any)
	if !ok {
		return nil, fmt.Errorf("expected array, got %T", decoded)
	}
	if len(arr) == 0 {
		return nil, fmt.Errorf("empty command")
	}
	parts := make([]string, len(arr))
	for i, v := range arr {
		s, ok := v.(string)
		if !ok {
			return nil, fmt.Errorf("expected bulk string at index %d, got %T", i, v)
		}
		parts[i] = s
	}
	return &Command{
		Name: strings.ToUpper(parts[0]),
		Args: parts[1:],
	}, nil
}
