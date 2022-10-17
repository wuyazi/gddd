package gddd

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestNewRocketMqEventBus(t *testing.T) {
	config := RocketMqEventProducerConfig{"apple", "community", []string{"127.0.0.1:9876"}, nil}
	gotBus, err := NewRocketMqEventProducer(context.TODO(), config)
	if err != nil {
		t.Errorf("NewRocketMqEventProducer() error = %v", err)
		return
	}
	fmt.Println(gotBus)
}

func Test_rocketmqEventBus_Start(t *testing.T) {
	config := RocketMqEventProducerConfig{"apple", "community", []string{"127.0.0.1:9876"}, nil}
	gotBus, err := NewRocketMqEventProducer(context.TODO(), config)
	if err != nil {
		t.Errorf("NewRocketMqEventProducer() error = %v", err)
		return
	}
	gotBus.Start()
}

func Test_rocketmqEventBus_Send(t *testing.T) {
	config := RocketMqEventProducerConfig{"apple", "community", []string{"127.0.0.1:9876"}, nil}
	gotBus, err := NewRocketMqEventProducer(context.TODO(), config)
	if err != nil {
		t.Errorf("NewRocketMqEventProducer() error = %v", err)
		return
	}
	event := SampleDomainEvent{aggregateId: 1, aggregateName: "book", eventId: 1, eventName: "BookCreated", eventBody: BookCreated{Book: "aa", Price: 2, CreateTime: time.Now().UTC()}}
	msg, _ := newDomainEventMessage(&event)
	err = gotBus.Send(context.TODO(), msg)
	if err != nil {
		t.Errorf("send error = %v", err)
		return
	}
}

func Test_rocketmqEventBus_Recv(t *testing.T) {
	config := RocketMqEventProducerConfig{"apple", "community", []string{"127.0.0.1:9876"}, nil}
	gotBus, err := NewRocketMqEventProducer(context.TODO(), config)
	if err != nil {
		t.Errorf("NewRocketMqEventProducer() error = %v", err)
		return
	}
	gotBus.Start()
	select {}
}
