package gddd

import "context"

type Command interface {
}

type AbstractCommand struct {
	AggregateId string
}

type CommandHandle func(ctx context.Context, command Command) (result interface{}, err error)
