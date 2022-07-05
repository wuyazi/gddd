package gddd

import (
	"context"
	"database/sql"
)

type Context struct {
	ctx context.Context
	tx  *sql.Tx
}

func (c *Context) Apply(aggregate Aggregate, change aggregateChange) error {
	ApplyAggregateChange(c.ctx, aggregate, change)
	return nil
}
