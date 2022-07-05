package gddd

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestNewRocketMqEventBus(t *testing.T) {
	config := RocketMqEventBusConfig{"apple", "community", []string{"127.0.0.1:9876"}, nil}
	gotBus, err := NewRocketMqEventBus(context.TODO(), config)
	if err != nil {
		t.Errorf("NewRocketMqEventBus() error = %v", err)
		return
	}
	fmt.Println(gotBus)
}

func Test_rocketmqEventBus_Start(t *testing.T) {
	config := RocketMqEventBusConfig{"apple", "community", []string{"127.0.0.1:9876"}, nil}
	gotBus, err := NewRocketMqEventBus(context.TODO(), config)
	if err != nil {
		t.Errorf("NewRocketMqEventBus() error = %v", err)
		return
	}
	err = gotBus.Start(context.TODO())
	if err != nil {
		t.Errorf("start error = %v", err)
		return
	}
}

func Test_rocketmqEventBus_Send(t *testing.T) {
	config := RocketMqEventBusConfig{"apple", "community", []string{"127.0.0.1:9876"}, nil}
	gotBus, err := NewRocketMqEventBus(context.TODO(), config)
	if err != nil {
		t.Errorf("NewRocketMqEventBus() error = %v", err)
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
	config := RocketMqEventBusConfig{"apple", "community", []string{"127.0.0.1:9876"}, nil}
	gotBus, err := NewRocketMqEventBus(context.TODO(), config)
	if err != nil {
		t.Errorf("NewRocketMqEventBus() error = %v", err)
		return
	}
	err = gotBus.Start(context.TODO())
	if err != nil {
		t.Errorf("start error = %v", err)
		return
	}
	select {}
}
