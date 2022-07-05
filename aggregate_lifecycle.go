package gddd

import (
	"errors"
	"strings"
	"sync"
)

type aggregateLifecycle struct {
	mutex        sync.Mutex
	domainEvents []DomainEvent
}

type aggregateChange interface{}

func (c *aggregateLifecycle) apply(agg Aggregate, aggChange aggregateChange) {
	c.mutex.Lock()
	if aggChange == nil {
		c.mutex.Unlock()
		panic(errors.New("aggregate apply failed, aggregateChange is nil"))
		return
	}
	eventName := strings.TrimSpace(getAggregateChangeName(aggChange))
	if eventName == "" {
		c.mutex.Unlock()
		panic(errors.New("aggregate apply failed, eventName is empty"))
		return
	}
	domainEvent := SampleDomainEvent{
		aggregateId:   agg.Identifier(),
		aggregateName: getAggregateName(agg),
		eventName:     eventName,
		eventBody:     aggChange,
	}
	domainEvent.initEventId()
	err := handleAppliedDomainEvent(agg, &domainEvent)
	if err != nil {
		c.mutex.Unlock()
		panic(errors.New("aggregate apply failed, apply domain event failed"))
		return
	}
	c.domainEvents = append(c.domainEvents, &domainEvent)
	c.mutex.Unlock()
	return
}

func (c *aggregateLifecycle) getDomainEvents() []DomainEvent {
	return c.domainEvents
}

func (c *aggregateLifecycle) cleanDomainEvents() {
	c.mutex.Lock()
	c.domainEvents = make([]DomainEvent, 0, 1)
	c.mutex.Unlock()
}
