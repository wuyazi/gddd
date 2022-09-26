package gddd

import (
	"context"
)

type Context struct {
	ctx        context.Context
	repository *Repository
}

func NewContext(ctx context.Context, repository *Repository) Context {
	return Context{ctx: ctx, repository: repository}
}

func (c *Context) Load(aggregate Aggregate) (has bool, err error) {
	return c.repository.Load(c.ctx, aggregate)
}

func (c *Context) Save(aggregates ...Aggregate) (ok bool, err error) {
	return c.repository.Save(c.ctx, aggregates...)
}

func (c *Context) Apply(aggregate Aggregate, change aggregateChange) {
	ApplyAggregateChange(c.ctx, aggregate, change)
	return
}
