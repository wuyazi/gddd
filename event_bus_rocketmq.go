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

type RocketMqEventBusConfig struct {
	DomainName    string
	SubDomainName string
	NameServers   []string
	EventStore    *EventStore
}

func NewRocketMqEventBus(ctx context.Context, config RocketMqEventBusConfig) (bus EventBus, err error) {
	domainName := strings.TrimSpace(config.DomainName)
	if domainName == "" {
		err = fmt.Errorf("new rocketmq event bus failed, DomainName is empty")
		return
	}
	subDomainName := strings.TrimSpace(config.SubDomainName)
	if subDomainName == "" {
		err = fmt.Errorf("new rocketmq event bus failed, SubDomainName is empty")
		return
	}
	if config.NameServers == nil || len(config.NameServers) == 0 {
		err = fmt.Errorf("new rocketmq event bus failed, NameServers is nil or empty")
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
	err = p.Start()
	if err != nil {
		fmt.Printf("start producer error: %s\n", err.Error())
		panic(err)
	}

	// consumer
	c, _ := rocketmq.NewPushConsumer(
		consumer.WithGroupName("testGroup"),
		consumer.WithNsResolver(primitive.NewPassthroughResolver(config.NameServers)),
	)
	if err != nil {
		fmt.Printf("new consumer error: %s\n", err.Error())
		panic(err)
	}

	consumers := make(map[string]EventHandle)
	bus0 := &rocketmqEventBus{
		name: fmt.Sprintf("%s_%s", config.DomainName, config.SubDomainName),
		//running:   NewAtomicSwitch(),
		brokers:   config.NameServers,
		producer:  p,
		consumer:  c,
		consumers: consumers,
	}
	bus = bus0
	return
}

type rocketmqEventBus struct {
	name      string
	brokers   []string
	producer  rocketmq.TransactionProducer
	consumer  rocketmq.PushConsumer
	consumers map[string]EventHandle
}

func (bus *rocketmqEventBus) Name() string {
	return bus.name
}

func (bus *rocketmqEventBus) Send(ctx context.Context, eventMessages ...DomainEventMessage) (err error) {
	if bus.producer == nil {
		err = fmt.Errorf("rocketmq event bus send event failed, producer is nil")
		return
	}
	if eventMessages == nil || len(eventMessages) == 0 {
		err = fmt.Errorf("rocketmq event bus send event failed, eventMessages is nil or empty")
		return
	}
	msgs := make([]*primitive.Message, 0, 1)
	for _, eventMessage := range eventMessages {
		var messageBody []byte
		messageBody, err = json.Marshal(eventMessage)
		if err != nil {
			return
		}
		msg := primitive.NewMessage(eventMessage.TopicName(bus.name), messageBody)
		msgs = append(msgs, msg)
	}
	res, err := bus.producer.SendMessageInTransaction(ctx, msgs...)
	if err != nil {
		fmt.Printf("send message error: %s\n", err)
	} else {
		fmt.Printf("send message success: result=%s\n", res.String())
	}
	return
}

func (bus *rocketmqEventBus) Recv(_ context.Context, topic string, handle EventHandle) (err error) {
	topic = strings.TrimSpace(topic)
	if topic == "" {
		err = fmt.Errorf("rocketmq event bus recv event failed, topic is empty")
		return
	}
	if handle == nil {
		err = fmt.Errorf("rocketmq event bus recv event failed, handle is nil")
		return
	}
	bus.consumers[topic] = handle
	return
}

func (bus *rocketmqEventBus) Start(ctx context.Context) (err error) {
	go func() {
		for {
			err = bus.consumer.Subscribe("DefaultCluster", consumer.MessageSelector{}, func(ctx context.Context,
				msgs ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
				for i := range msgs {
					fmt.Printf("subscribe callback: %v \n", msgs[i])
				}
				//这个相当于消费者 消息ack，如果失败可以返回 consumer.ConsumeRetryLater
				return consumer.ConsumeSuccess, nil
			})
			if err != nil {
				fmt.Printf("consume error: %s\n", err.Error())
			}
		}
	}()
	return nil
}

func (bus *rocketmqEventBus) Shutdown() {
	return
}

func (bus *rocketmqEventBus) Await() {
	return
}

func (bus *rocketmqEventBus) Close(ctx context.Context) (err error) {
	return
}
