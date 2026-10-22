package cluster_test

import (
	"fmt"
	"strings"

	"github.com/vinayhpandya/rediska/cluster"
)

// Every key maps to one of 16384 slots. Keys that share a {hash tag} always
// land in the same slot, so they can be used together in one command.
func ExampleKeyHashSlot() {
	fmt.Println(cluster.KeyHashSlot("name"))
	fmt.Println(cluster.KeyHashSlot("age"))
	fmt.Println(cluster.KeyHashSlot("{user1}.name"), cluster.KeyHashSlot("{user1}.email"))
	// Output:
	// 5798
	// 741
	// 8106 8106
}

// A layout lists every node and the slots it owns. The node finds itself
// by its port, and Owner says which node serves a slot.
func ExampleParseLayout() {
	conf := `
node1  127.0.0.1:7001  0-5460
node2  127.0.0.1:7002  5461-10922
node3  127.0.0.1:7003  10923-16383
`
	layout, err := cluster.ParseLayout(strings.NewReader(conf), 7001)
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	slot := cluster.KeyHashSlot("name")
	owner := layout.Owner(slot)
	fmt.Println("I am", layout.Self.Name)
	fmt.Println("slot", slot, "is owned by", owner.Name, "at", owner.Addr)
	fmt.Println("is it mine?", owner == layout.Self)
	// Output:
	// I am node1
	// slot 5798 is owned by node2 at 127.0.0.1:7002
	// is it mine? false
}
