package gddd

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/dtm-labs/client/dtmcli"
	"github.com/dtm-labs/client/dtmgrpc"
	"github.com/lithammer/shortuuid/v3"
	"strings"
)

type DtmEventProducerConfig struct {
	DomainName    string
	SubDomainName string
	NameServers   []string
	EventStore    *EventStore
}

type DtmEventProducer struct {
	Name       string
	Brokers    []string
	Producer   *dtmcli.Msg
	EventStore EventStore
}

func NewDtmEventProducer(ctx context.Context, config DtmEventProducerConfig) (eventProducer DtmEventProducer, err error) {
	domainName := strings.TrimSpace(config.DomainName)
	if domainName == "" {
		err = fmt.Errorf("new dtm event producer failed, DomainName is empty")
		return
	}
	subDomainName := strings.TrimSpace(config.SubDomainName)
	if subDomainName == "" {
		err = fmt.Errorf("new dtm event producer failed, SubDomainName is empty")
		return
	}
	if config.NameServers == nil || len(config.NameServers) == 0 {
		err = fmt.Errorf("new dtm event producer failed, NameServers is nil or empty")
		return
	}
	eventProducer = DtmEventProducer{
		Name:       fmt.Sprintf("%s_%s", config.DomainName, config.SubDomainName),
		Brokers:    config.NameServers,
		Producer:   nil,
		EventStore: *config.EventStore,
	}
	return
}

func ExecuteLocalTransaction(ctx context.Context, es EventStore, eventsMessages []DomainEventMessage) error {
	storedEvents := make([]StoredEvent, 0, 1)
	for _, eventMessage := range eventsMessages {
		storedEvent, err := newJsonStoredEvent(eventMessage.AggregateId, eventMessage.AggregateName, eventMessage.EventId, eventMessage.EventName, eventMessage.EventBody)
		if err != nil {
			return fmt.Errorf("newJsonStoredEvent error")
		}
		storedEvents = append(storedEvents, storedEvent)
	}
	storeEventsErr := es.StoreEvents(ctx, storedEvents)
	if storeEventsErr != nil {
		return fmt.Errorf("storeEventsErr error")
	}
	return nil
}

func (p *DtmEventProducer) Send(ctx context.Context, eventMessages ...DomainEventMessage) (err error) {
	if eventMessages == nil || len(eventMessages) == 0 {
		err = fmt.Errorf("dtm event producer send event failed, eventMessages is nil or empty")
		return
	}
	//dtmMsg := dtmcli.NewMsg("http://localhost:36789/api/dtmsvr", shortuuid.New())
	dtmMsg := dtmgrpc.NewMsgGrpc("localhost:36790", shortuuid.New())
	for _, eventMessage := range eventMessages {
		var messageBody []byte
		messageBody, err = json.Marshal(eventMessage)
		if err != nil {
			return
		}
		fmt.Errorf("%+v", messageBody)
		//dtmMsg = dtmMsg.Add("http://localhost:8081/api/busi/TransIn", &messageBody)
		msg := newDomainEventMessageProto(eventMessage)
		dtmMsg = dtmMsg.Add("localhost:8080/proto.userQuery/insertUser", &msg)
	}
	err = dtmMsg.DoAndSubmitDB("localhost:8081/busi.Busi/QueryPreparedB", p.EventStore.GetDB(ctx), func(tx *sql.Tx) error {
		// TODO use tx
		return ExecuteLocalTransaction(ctx, p.EventStore, eventMessages)
	})
	if err != nil {
		return err
	}
	return
}

type DtmEventConsumerConfig struct {
	DomainName  string
	GroupName   string
	NameServers []string
}

type DtmEventConsumer struct {
	DomainName string
	GroupName  string
	Consumer   rocketmq.PushConsumer
}

func NewDtmEventConsumer(ctx context.Context, config DtmEventConsumerConfig) (eventConsumer *DtmEventConsumer, err error) {
	eventConsumer.DomainName = config.DomainName
	eventConsumer.GroupName = config.GroupName
	c, err := rocketmq.NewPushConsumer(
		consumer.WithGroupName(eventConsumer.GroupName),
		consumer.WithNsResolver(primitive.NewPassthroughResolver(config.NameServers)),
	)
	eventConsumer.Consumer = c
	return
}

func (c *DtmEventConsumer) Start() {
	err := c.Consumer.Start()
	if err != nil {
		fmt.Printf("DtmEventConsumer start error: %v\n", err)
		panic(err)
	}
}

func (c *DtmEventConsumer) Stop() {
	err := c.Consumer.Shutdown()
	if err != nil {
		fmt.Printf("DtmEventConsumer stop error: %v\n", err)
		panic(err)
	}
}

func (c *DtmEventConsumer) Subscribe(topicName string, change aggregateChange, eventHandle EventHandle) {
	topicName = strings.TrimSpace(topicName)
	if topicName == "" {
		err := fmt.Errorf("DtmEventConsumer subscribe event failed, topicName is empty")
		panic(err)
	}
	if change == nil {
		err := fmt.Errorf("DtmEventConsumer subscribe event failed, change is nil")
		panic(err)
	}
	if eventHandle == nil {
		err := fmt.Errorf("DtmEventConsumer subscribe event failed, eventHandle is nil")
		panic(err)
	}
	err := c.Consumer.Subscribe(topicName, consumer.MessageSelector{},
		func(ctx context.Context, msgs ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
			for _, msg := range msgs {
				if topicName == msg.Message.Topic {
					domainEventMessage := new(DomainEventMessage)
					err := json.Unmarshal(msg.Message.Body, domainEventMessage)
					if err != nil {
						return consumer.ConsumeRetryLater, err
					}
					if getAggregateChangeName(change) == domainEventMessage.EventName {
						var event SampleDomainEvent
						// TODO: change is read only?
						newChange := change
						event, err = newSampleDomainEvent(*domainEventMessage, newChange)
						if err != nil {
							return 0, err
						}
						err, _ = eventHandle(context.TODO(), &event)
						if err != nil {
							return consumer.ConsumeRetryLater, err
						}
					}
				}
			}
			return consumer.ConsumeSuccess, nil
		})
	if err != nil {
		panic(nil)
	}
}