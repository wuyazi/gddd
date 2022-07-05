package gddd

import (
	"context"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"sync"
)

type DemoListener struct {
	es               *EventStore
	localTrans       *sync.Map
	transactionIndex int32
}

func NewDemoListener(eventStore *EventStore) *DemoListener {
	return &DemoListener{
		es:         eventStore,
		localTrans: new(sync.Map),
	}
}

// ExecuteLocalTransaction 在 SendMessageInTransaction 方法调用
// 如果返回 primitive.UnknowState 那么 brocker 就会调用 CheckLocalTransaction 方法检查消息状态
// 如果返回 primitive.CommitMessageState 和 primitive.RollbackMessageState 则不会调用 CheckLocalTransaction
func (dl *DemoListener) ExecuteLocalTransaction(ctx context.Context, msgs ...*primitive.Message) primitive.LocalTransactionState {
	storedEvents := make([]StoredEvent, 0, 1)
	for _, msg := range msgs {
		eventMessage := new(DomainEventMessage)
		err := eventMessage.Decode(msg.Body)
		if err != nil {
			return primitive.RollbackMessageState
		}
		var storedEvent StoredEvent
		storedEvent, err = newJsonStoredEvent(eventMessage.AggregateId, eventMessage.AggregateName, eventMessage.EventId, eventMessage.EventName, eventMessage.EventBody)
		if err != nil {
			return primitive.RollbackMessageState
		}
		storedEvents = append(storedEvents, storedEvent)
	}
	storeEventsErr := (*dl.es).StoreEvents(ctx, storedEvents)
	if storeEventsErr != nil {
		//err := fmt.Errorf("aggregates repository save failed, store domain events failed, %v", storeEventsErr)
		return primitive.RollbackMessageState
	}
	return primitive.CommitMessageState
}

func (dl *DemoListener) CheckLocalTransaction(msges ...*primitive.MessageExt) primitive.LocalTransactionState {
	storedEvents := make([]StoredEvent, 0, 1)
	for _, msge := range msges {
		eventMessage := new(DomainEventMessage)
		err := eventMessage.Decode(msge.Message.Body)
		if err != nil {
			return primitive.RollbackMessageState
		}
		var storedEvent StoredEvent
		storedEvent, err = newJsonStoredEvent(eventMessage.AggregateId, eventMessage.AggregateName, eventMessage.EventId, eventMessage.EventName, eventMessage.EventBody)
		if err != nil {
			return primitive.RollbackMessageState
		}
		storedEvents = append(storedEvents, storedEvent)
	}
	err := (*dl.es).CheckEvents(context.TODO(), storedEvents)
	if err != nil {
		// todo
		return primitive.RollbackMessageState
	}
	return primitive.CommitMessageState
}
