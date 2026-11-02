package core

import (
	"path/filepath"
	"reflect"
	"testing"
)

// A transaction whose EXEC never reached the file (the server crashed
// while writing it) must not be replayed at all. Complete transactions,
// and commands outside transactions, are replayed as usual.
func TestRecoverDropsUnfinishedTransaction(t *testing.T) {
	a, err := NewAOF(filepath.Join(t.TempDir(), "appendonly.aof"))
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()

	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(a.Write([]string{"SET", "before", "1"}))
	must(a.WriteTransaction([][]string{{"SET", "a", "1"}, {"SET", "b", "2"}}))
	must(a.Write([]string{"SET", "after", "1"}))
	// A crash part-way through the next transaction: MULTI and one
	// command made it to the file, the EXEC didn't.
	must(a.Write([]string{"MULTI"}))
	must(a.Write([]string{"SET", "half", "1"}))

	var got [][]string
	must(a.Recover(func(args []string) { got = append(got, args) }))

	want := [][]string{
		{"SET", "before", "1"},
		{"SET", "a", "1"},
		{"SET", "b", "2"},
		{"SET", "after", "1"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("replayed %q\nwant     %q", got, want)
	}
}
