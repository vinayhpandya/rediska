package cluster

import (
	"fmt"

	"github.com/vinayhpandya/rediska/core"
)

type Message struct {
	Type string
	ID   string
	Name string
	Addr string
}

func (m *Message) encode() []byte {
	return core.EncodeArray([]string{m.Type, m.ID, m.Name, m.Addr})
}

func decodeMessage(v any) (Message, error) {
	arr, ok := v.([]any)
	if !ok || len(arr) != 4 {
		return Message{}, fmt.Errorf("cluster bus: want an array of 4, got %v", v)
	}
	parts := make([]string, 4)
	for i, x := range arr {
		s, ok := x.(string)
		if !ok {
			return Message{}, fmt.Errorf("cluster bus: element %d is not a string", i)
		}
		parts[i] = s
	}
	m := Message{Type: parts[0], ID: parts[1], Name: parts[2], Addr: parts[3]}
	if m.Type != "PING" && m.Type != "PONG" {
		return Message{}, fmt.Errorf("cluster bus: unknown message type %q", m.Type)
	}
	return m, nil
}
