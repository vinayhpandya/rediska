package core

import (
	"bufio"
	"fmt"
	"os"
)

type AOF struct {
	file *os.File
}

func NewAOF(path string) (*AOF, error) {
	f, ok := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	if ok != nil {
		return nil, fmt.Errorf("Error while reading AOF file %w", ok)
	}
	return &AOF{file: f}, nil
}

func (a *AOF) Write(args []string) error {
	_, err := a.file.Write(EncodeArray(args))
	return err
}

func (a *AOF) Recover(fn func(args []string)) error {
	if _, err := a.file.Seek(0, 0); err != nil {
		return fmt.Errorf("AOF seek: %w", err)
	}

	reader := bufio.NewReader(a.file)
	for {
		value, err := Decode(reader)
		if err != nil {
			break
		}
		arr, ok := value.([]any)
		if !ok {
			continue // skip anything that isn't an array
		}

		// Convert []any → []string to pass to the command executor.
		args := make([]string, len(arr))
		for i, v := range arr {
			s, ok := v.(string)
			if !ok {
				continue
			}
			args[i] = s
		}
		fn(args)
	}
	return nil
}
func (a *AOF) Close() error {
	return a.file.Close()
}
