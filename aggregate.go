package gddd

import "context"

func NewAggregateId() (id int64) {
	id = node.Generate().Int64()
	return
}

func ApplyAggregateChange(ctx context.Context, aggregate Aggregate, change aggregateChange) {
	aggregate.Apply(aggregate, change)
}

type Aggregate interface {
	InitId()
	Identifier() (id int64)
	Apply(agg Aggregate, event aggregateChange)
	Applied() (events []DomainEvent)
}

type AbstractAggregate struct {
	Id        int64 `json:"id"`
	lifecycle aggregateLifecycle
}

func (a *AbstractAggregate) InitId() {
	if a.Id == 0 {
		a.Id = NewAggregateId()
	}
	return
}

func (a *AbstractAggregate) Identifier() (id int64) {
	id = a.Id
	return
}

func (a *AbstractAggregate) Apply(agg Aggregate, aggChange aggregateChange) {
	a.lifecycle.apply(agg, aggChange)
	return
}

func (a *AbstractAggregate) Applied() (events []DomainEvent) {
	events = a.lifecycle.getDomainEvents()
	return
}
