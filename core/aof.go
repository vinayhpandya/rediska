package core

import (
	"bufio"
	"fmt"
	"os"
)

// AOF is an append-only file: each successful write command is appended
// to it in RESP format, and replaying the file at startup rebuilds the
// data. It is not safe for concurrent use.
type AOF struct {
	file *os.File
}

// Sync flushes the file to disk with fsync. Until then, recent writes may
// only be in the operating system's cache and can be lost if the machine
// crashes. The server calls Sync when it shuts down.
func (a *AOF) Sync() error {
	return a.file.Sync()
}

// NewAOF opens the append-only file at path, creating it if it doesn't
// exist. Writes are always added to the end of the file.
func NewAOF(path string) (*AOF, error) {
	f, ok := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if ok != nil {
		return nil, fmt.Errorf("Error while reading AOF file %w", ok)
	}
	return &AOF{file: f}, nil
}

// Write appends one command, such as ["SET", "name", "vinay"], to the file
// as a RESP array. It doesn't call Sync.
func (a *AOF) Write(args []string) error {
	_, err := a.file.Write(EncodeArray(args))
	return err
}

// Recover replays the file from the beginning, calling fn with each
// command's arguments, name first. Entries that aren't arrays are skipped.
// It stops quietly at the first entry it can't decode, so a file cut short
// by a crash still loads every complete command before that point. It only
// returns an error if it can't move to the start of the file.
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

// Close closes the file. Call Sync first to make sure everything written
// is on disk.
func (a *AOF) Close() error {
	return a.file.Close()
}
