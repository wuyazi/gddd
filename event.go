package gddd

import (
	"encoding/json"
	"fmt"
	"time"
)

func NewDomainEventId() (id int64) {
	id = node.Generate().Int64()
	return
}

type DomainEvent interface {
	AggregateId() int64
	AggregateName() string
	EventId() int64
	EventName() string
	EventBody() interface{}
	EventBodyRaw() ([]byte, error)
	EventCreateTime() time.Time
	initEventId()
}

type RegularEvent interface {
}

type SampleDomainEvent struct {
	aggregateId   int64
	aggregateName string
	eventId       int64
	eventName     string
	eventBody     interface{}
}

func (s *SampleDomainEvent) initEventId() {
	if s.eventId == 0 {
		s.eventId = NewDomainEventId()
	}
	return
}

func (s *SampleDomainEvent) AggregateId() (id int64) {
	id = s.aggregateId
	return
}

func (s *SampleDomainEvent) AggregateName() (name string) {
	name = s.aggregateName
	return
}

func (s *SampleDomainEvent) EventId() (id int64) {
	id = s.eventId
	return
}

func (s *SampleDomainEvent) EventName() (name string) {
	name = s.eventName
	return
}

func (s *SampleDomainEvent) EventBody() (body interface{}) {
	body = s.eventBody
	return
}

func (s *SampleDomainEvent) EventCreateTime() (createTime time.Time) {
	createTime = NodeTime(s.eventId)
	return
}

func (s *SampleDomainEvent) EventBodyRaw() (bodyRaw []byte, err error) {
	bodyRaw, err = json.Marshal(s.eventBody)
	if err != nil {
		err = fmt.Errorf("marshal domain event failed, %v", err)
		return
	}
	return
}

func newSampleDomainEvent(eventMessage DomainEventMessage, change aggregateChange) (domainEvent SampleDomainEvent, err error) {
	domainEvent = SampleDomainEvent{
		aggregateId:   eventMessage.AggregateId,
		aggregateName: eventMessage.AggregateName,
		eventId:       eventMessage.EventId,
		eventName:     eventMessage.EventName,
	}
	err = json.Unmarshal(eventMessage.EventBody, &change)
	if err != nil {
		return
	}
	return
}
