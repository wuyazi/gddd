package gddd

import (
	"encoding/json"
	"fmt"
	"github.com/jinzhu/copier"
)

func handleAppliedDomainEventRecursively(aggregate Aggregate, events []DomainEvent) (err error) {
	if events == nil || len(events) == 0 {
		return
	}
	for _, event := range events {
		err = handleAppliedDomainEvent(aggregate, event)
		if err != nil {
			return
		}
	}
	return
}

func handleStoredEventRecursively(aggregate Aggregate, events []StoredEvent) (err error) {
	if events == nil || len(events) == 0 {
		return
	}
	aggregateName := getAggregateName(aggregate)
	aggregateId := aggregate.Identifier()

	for _, event := range events {
		eventId := event.EventId()
		if eventId == 0 {
			err = fmt.Errorf("aggregates handle recursively stored domain events failed, domain event id is 0, aggregateName is %s, aggregateId is %d", aggregateName, aggregateId)
			return
		}
		eventName := event.EventName()
		if eventName == "" {
			err = fmt.Errorf("aggregates handle recursively stored domain events failed, domain event name is empty, aggregateName is %s, aggregateId is %d", aggregateName, aggregateId)
			return
		}
		eventBody := event.EventBodyRaw()
		if eventBody == nil {
			err = fmt.Errorf("aggregates handle recursively stored domain events failed, domain event data is nil, aggregateName is %s, aggregateId is %d", aggregateName, aggregateId)
			return
		}
		err = handleStoredEvent(aggregate, event)
		if err != nil {
			return
		}
	}
	return
}

func handleAppliedDomainEvent(aggregate Aggregate, event DomainEvent) (err error) {
	err = copier.CopyWithOption(aggregate, event.EventBody(), copier.Option{IgnoreEmpty: false, DeepCopy: true})
	if err != nil {
		return
	}
	return
}

func handleStoredEvent(aggregate Aggregate, event StoredEvent) (err error) {
	err = json.Unmarshal(event.EventBodyRaw(), aggregate)
	if err != nil {
		return
	}
	return
}
