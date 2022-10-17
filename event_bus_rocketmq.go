package gddd

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/producer"
	"strings"
)

type RocketMqEventProducerConfig struct {
	DomainName    string
	SubDomainName string
	NameServers   []string
	EventStore    *EventStore
}

type RocketmqEventProducer struct {
	Name     string
	Brokers  []string
	Producer rocketmq.TransactionProducer
}

func NewRocketMqEventProducer(ctx context.Context, config RocketMqEventProducerConfig) (eventProducer RocketmqEventProducer, err error) {
	domainName := strings.TrimSpace(config.DomainName)
	if domainName == "" {
		err = fmt.Errorf("new rocketmq event producer failed, DomainName is empty")
		return
	}
	subDomainName := strings.TrimSpace(config.SubDomainName)
	if subDomainName == "" {
		err = fmt.Errorf("new rocketmq event producer failed, SubDomainName is empty")
		return
	}
	if config.NameServers == nil || len(config.NameServers) == 0 {
		err = fmt.Errorf("new rocketmq event producer failed, NameServers is nil or empty")
		return
	}
	p, err := rocketmq.NewTransactionProducer(
		NewDemoListener(config.EventStore),
		producer.WithNsResolver(primitive.NewPassthroughResolver(config.NameServers)),
		producer.WithRetry(1),
	)
	if err != nil {
		fmt.Printf("new producer error: %s\n", err.Error())
		panic(err)
	}
	eventProducer = RocketmqEventProducer{
		Name:     fmt.Sprintf("%s_%s", config.DomainName, config.SubDomainName),
		Brokers:  config.NameServers,
		Producer: p,
	}
	eventProducer.Producer = p
	return
}

func (p *RocketmqEventProducer) Start() {
	err := p.Producer.Start()
	if err != nil {
		fmt.Printf("start producer error: %s\n", err.Error())
		panic(err)
	}
}

func (p *RocketmqEventProducer) Stop() {
	err := p.Producer.Shutdown()
	if err != nil {
		fmt.Printf("stop producer error: %s\n", err.Error())
		panic(err)
	}
	return
}

func (p *RocketmqEventProducer) Send(ctx context.Context, eventMessages ...DomainEventMessage) (err error) {
	if p.Producer == nil {
		err = fmt.Errorf("rocketmq event producer send event failed, Producer is nil")
		return
	}
	if eventMessages == nil || len(eventMessages) == 0 {
		err = fmt.Errorf("rocketmq event producer send event failed, eventMessages is nil or empty")
		return
	}
	msgs := make([]*primitive.Message, 0, 1)
	for _, eventMessage := range eventMessages {
		var messageBody []byte
		messageBody, err = json.Marshal(eventMessage)
		if err != nil {
			return
		}
		msg := primitive.NewMessage(eventMessage.TopicName(p.Name), messageBody)
		msgs = append(msgs, msg)
	}
	res, err := p.Producer.SendMessageInTransaction(ctx, msgs...)
	if err != nil {
		fmt.Printf("send message error: %s\n", err)
	} else {
		fmt.Printf("send message success: result=%s\n", res.String())
	}
	return
}

type RocketMqEventConsumerConfig struct {
	DomainName  string
	GroupName   string
	NameServers []string
}

type RocketMqEventConsumer struct {
	DomainName string
	GroupName  string
	Consumer   rocketmq.PushConsumer
}

func NewRocketMqEventConsumer(ctx context.Context, config RocketMqEventConsumerConfig) (eventConsumer *RocketMqEventConsumer, err error) {
	eventConsumer.DomainName = config.DomainName
	eventConsumer.GroupName = config.GroupName
	c, err := rocketmq.NewPushConsumer(
		consumer.WithGroupName(eventConsumer.GroupName),
		consumer.WithNsResolver(primitive.NewPassthroughResolver(config.NameServers)),
	)
	eventConsumer.Consumer = c
	return
}

func (c *RocketMqEventConsumer) Start() {
	err := c.Consumer.Start()
	if err != nil {
		fmt.Printf("RocketMqEventConsumer start error: %v\n", err)
		panic(err)
	}
}

func (c *RocketMqEventConsumer) Stop() {
	err := c.Consumer.Shutdown()
	if err != nil {
		fmt.Printf("RocketMqEventConsumer stop error: %v\n", err)
		panic(err)
	}
}

func (c *RocketMqEventConsumer) Subscribe(topicName string, change aggregateChange, eventHandle EventHandle) {
	topicName = strings.TrimSpace(topicName)
	if topicName == "" {
		err := fmt.Errorf("RocketMqEventConsumer subscribe event failed, topicName is empty")
		panic(err)
	}
	if change == nil {
		err := fmt.Errorf("RocketMqEventConsumer subscribe event failed, change is nil")
		panic(err)
	}
	if eventHandle == nil {
		err := fmt.Errorf("RocketMqEventConsumer subscribe event failed, eventHandle is nil")
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
