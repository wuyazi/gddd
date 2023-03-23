package gddd

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/wuyazi/gddd/proto"
	"strings"
)

// EventHandle handle event, if return err is not nil, it will reject event,
// if want to requeue, then make the requeue be true
type EventHandle func(ctx context.Context, domainEvent DomainEvent) (err error, requeue bool)

type EventBus interface {
	Name() string
	Send(ctx context.Context, eventMessages ...DomainEventMessage) (err error)
	Recv(ctx context.Context, topic string, handle EventHandle) (err error)
	Start(ctx context.Context) (err error)
	Shutdown()
	Await()
	Close(ctx context.Context) (err error)
}

type DomainEventMessage struct {
	AggregateId   int64  `json:"aggregate_id"`
	AggregateName string `json:"aggregate_name"`
	EventName     string `json:"event_name"`
	EventId       int64  `json:"event_id"`
	EventBody     []byte `json:"event_body"`
}

func (msg *DomainEventMessage) TopicName(eventBusName string) string {
	topic := fmt.Sprintf("%s_%s", eventBusName, strings.TrimSuffix(strings.ToLower(msg.AggregateName), "aggregate"))
	return topic
}

func (msg *DomainEventMessage) Decode(byteData []byte) (err error) {
	err = json.Unmarshal(byteData, msg)
	return
}

func newDomainEventMessage(event DomainEvent) (msg DomainEventMessage, err error) {
	eventBodyRaw, err := event.EventBodyRaw()
	if err != nil {
		return
	}
	msg = DomainEventMessage{
		AggregateId:   event.AggregateId(),
		AggregateName: event.AggregateName(),
		EventName:     event.EventName(),
		EventId:       event.EventId(),
		EventBody:     eventBodyRaw,
	}
	return
}

func newDomainEventMessageProto(msg0 DomainEventMessage) (msg proto.DomainEventMessageProto) {
	msg = proto.DomainEventMessageProto{
		AggregateId:   msg0.AggregateId,
		AggregateName: msg0.AggregateName,
		EventName:     msg0.EventName,
		EventId:       msg0.EventId,
		EventBody:     msg0.EventBody,
	}
	return
}
