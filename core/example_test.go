package core_test

import (
	"fmt"

	"github.com/vinayhpandya/rediska/core"
)

// The encoders return raw RESP bytes; %q shows the \r\n line endings.
func ExampleEncodeBulkString() {
	fmt.Printf("%q\n", core.EncodeBulkString("vinay"))
	fmt.Printf("%q\n", core.EncodeSimpleString("OK"))
	fmt.Printf("%q\n", core.EncodeInteger(42))
	fmt.Printf("%q\n", core.EncodeNullBulkString())
	// Output:
	// "$5\r\nvinay\r\n"
	// "+OK\r\n"
	// ":42\r\n"
	// "$-1\r\n"
}

// EncodeArray is the format clients use to send commands.
func ExampleEncodeArray() {
	fmt.Printf("%q\n", core.EncodeArray([]string{"SET", "name", "vinay"}))
	// Output:
	// "*3\r\n$3\r\nSET\r\n$4\r\nname\r\n$5\r\nvinay\r\n"
}

// EncodeRawArray takes already-encoded items, so it can mix types and nest
// arrays. This builds [0, 5460, ["127.0.0.1", 7001]].
func ExampleEncodeRawArray() {
	owner := core.EncodeRawArray([][]byte{
		core.EncodeBulkString("127.0.0.1"),
		core.EncodeInteger(7001),
	})
	reply := core.EncodeRawArray([][]byte{
		core.EncodeInteger(0),
		core.EncodeInteger(5460),
		owner,
	})
	fmt.Printf("%q\n", reply)
	// Output:
	// "*3\r\n:0\r\n:5460\r\n*2\r\n$9\r\n127.0.0.1\r\n:7001\r\n"
}
