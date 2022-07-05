package gddd

import (
	"fmt"
	"testing"
	"time"
)

type BookAggregate struct {
	AbstractAggregate
	Book       string    `json:"book"`
	Price      int64     `json:"price"`
	CreateTime time.Time `json:"create_time"`
}

type BookCreated struct {
	Book       string    `json:"book"`
	Price      int64     `json:"price"`
	CreateTime time.Time `json:"create_time"`
}

type BookChangedPrice struct {
	Price int64 `json:"price"`
}

func Test_handleAggregate(t *testing.T) {
	time_, _ := time.Parse("2006-01-02T15:04:05", "2022-01-19T23:59:59")
	type args struct {
		aggregate Aggregate
		event     DomainEvent
	}
	tests := []struct {
		name string
		args args
	}{
		{
			"BookCreated",
			args{
				aggregate: &BookAggregate{AbstractAggregate: AbstractAggregate{Id: 1}},
				event:     &SampleDomainEvent{aggregateId: 1, aggregateName: "book", eventId: 1, eventName: "BookCreated", eventBody: BookCreated{Book: "aa", Price: 2, CreateTime: time.Now().UTC()}},
			},
		},
		{
			"BookChangedPrice",
			args{
				aggregate: &BookAggregate{AbstractAggregate: AbstractAggregate{Id: 1}, Book: "aa", Price: 2, CreateTime: time_},
				event:     &SampleDomainEvent{aggregateId: 1, aggregateName: "book", eventId: 2, eventName: "BookChangedPrice", eventBody: BookChangedPrice{Price: 3}},
			},
		},
		{
			"BookChangedPrice",
			args{
				aggregate: &BookAggregate{AbstractAggregate: AbstractAggregate{Id: 1}, Book: "aa", Price: 3, CreateTime: time_},
				event:     &SampleDomainEvent{aggregateId: 1, aggregateName: "book", eventId: 2, eventName: "BookChangedPrice", eventBody: BookChangedPrice{Price: 4}},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handleAppliedDomainEvent(tt.args.aggregate, tt.args.event)
			fmt.Println("--", tt.args.aggregate)
		})
	}
}
