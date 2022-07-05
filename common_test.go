package gddd

import (
	"fmt"
	"github.com/bwmarrin/snowflake"
	"testing"
)

func TestNodeTime(t *testing.T) {
	node, _ = snowflake.NewNode(1)
	time0 := NodeTime(node.Generate().Int64())
	fmt.Println("time is: ", time0)
}
