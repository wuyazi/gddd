package gddd

import (
	"context"
	"time"
)

type EventStore interface {
	Name() (name string)
	InitDomainEventStoreTable(ctx context.Context, aggregateName string)
	ReadEvents(ctx context.Context, aggregateName string, aggregateId int64, lastEventId int64) (events []StoredEvent, err error)
	StoreEvents(ctx context.Context, events []StoredEvent) (err error)
	CheckEvents(ctx context.Context, events []StoredEvent) (err error)
	MakeSnapshot(ctx context.Context, aggregate Aggregate) (err error)
	LoadSnapshot(ctx context.Context, aggregateId int64, aggregate Aggregate) (lastEventId int64, err error)
}

type StoredEvent interface {
	AggregateId() int64
	AggregateName() string
	EventId() int64
	EventName() string
	EventBodyRaw() []byte
	EventCreateTime() time.Time
}

type jsonStoredEvent struct {
	aggregateId   int64
	aggregateName string
	eventId       int64
	eventName     string
	eventBodyRaw  []byte
}

func newJsonStoredEvent(aggregateId int64, aggregateName string, eventId int64, eventName string, eventByte []byte) (event *jsonStoredEvent, err error) {
	event = &jsonStoredEvent{
		aggregateId:   aggregateId,
		aggregateName: aggregateName,
		eventId:       eventId,
		eventName:     eventName,
		eventBodyRaw:  eventByte,
	}
	return
}

func (e *jsonStoredEvent) AggregateId() int64 {
	return e.aggregateId
}

func (e *jsonStoredEvent) AggregateName() string {
	return e.aggregateName
}

func (e *jsonStoredEvent) EventId() int64 {
	return e.eventId
}

func (e *jsonStoredEvent) EventName() string {
	return e.eventName
}

func (e *jsonStoredEvent) EventBodyRaw() []byte {
	return e.eventBodyRaw
}

func (e *jsonStoredEvent) EventCreateTime() time.Time {
	return NodeTime(e.eventId)
}
