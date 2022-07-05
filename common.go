package gddd

import (
	"fmt"
	"github.com/bwmarrin/snowflake"
	"reflect"
	"time"
)

var node *snowflake.Node

func init() {
	// init snowflake
	var nodeErr error
	node, nodeErr = snowflake.NewNode(NodeNumber)
	if nodeErr != nil {
		fmt.Println(nodeErr)
		return
	}
}

func getAggregateName(a Aggregate) string {
	rt := reflect.TypeOf(a)
	if rt.Kind() == reflect.Ptr {
		rt = rt.Elem()
	}
	if rt.Kind() != reflect.Struct {
		panic("bad aggregate type")
	}
	return rt.Name()
}

func getAggregateChangeName(e aggregateChange) string {
	rt := reflect.TypeOf(e)
	if rt.Kind() == reflect.Ptr {
		rt = rt.Elem()
	}
	if rt.Kind() != reflect.Struct {
		panic("bad aggregateChange type")
	}
	return rt.Name()
}

func getEventName(e DomainEvent) string {
	rt := reflect.TypeOf(e)
	if rt.Kind() == reflect.Ptr {
		rt = rt.Elem()
	}
	if rt.Kind() != reflect.Struct {
		panic("bad event type")
	}
	return rt.Name()
}

func NodeTime(snowflakeId int64) time.Time {
	milliTimeStamp := (snowflakeId >> (snowflake.NodeBits + snowflake.StepBits)) + snowflake.Epoch
	return time.Unix(milliTimeStamp/1000, milliTimeStamp%1000*1000000)
}
