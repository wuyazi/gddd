package gddd

import (
	"context"
	"sync"
)

type localEventbusGroups struct {
	mutex         *sync.Mutex
	groups        map[string]chan *localEvent
	eventCountMap map[string]*sync.WaitGroup
}

func (g *localEventbusGroups) addGroup(name string, ch chan *localEvent, count *sync.WaitGroup) {
	g.mutex.Lock()
	defer g.mutex.Unlock()
	g.eventCountMap[name] = count
	g.groups[name] = ch
}

func (g *localEventbusGroups) removeGroup(name string) {
	g.mutex.Lock()
	defer g.mutex.Unlock()
	delete(g.groups, name)
	delete(g.eventCountMap, name)
	if len(g.groups) == 0 {
		_localEventbusGroups = nil
	}
}

func (g *localEventbusGroups) send(topic string, event interface{}) {
	g.mutex.Lock()
	defer g.mutex.Unlock()
	e := &localEvent{
		topic: topic,
		value: event,
	}
	for name, events := range g.groups {
		events <- e
		g.eventCountMap[name].Add(1)
	}
}

var _localEventbusGroups *localEventbusGroups

func NewLocalEventBus(name string) (bus EventBus) {
	if _localEventbusGroups == nil {
		_localEventbusGroups = &localEventbusGroups{
			mutex:         &sync.Mutex{},
			groups:        make(map[string]chan *localEvent),
			eventCountMap: make(map[string]*sync.WaitGroup),
		}
	}
	eventsCh := make(chan *localEvent, 1024)
	eventCount := &sync.WaitGroup{}
	_localEventbusGroups.addGroup(name, eventsCh, eventCount)
	bus = &LocalEventBus{
		eventCount: eventCount,
		name:       name,
		eventsCh:   eventsCh,
		consumers:  make(map[string]EventHandle),
	}

	return
}

type localEvent struct {
	topic string
	value interface{}
}

type LocalEventBus struct {
	eventCount *sync.WaitGroup
	name       string
	eventsCh   chan *localEvent
	consumers  map[string]EventHandle
}

func (bus *LocalEventBus) Name() string {
	return bus.name
}

func (bus *LocalEventBus) Send(ctx context.Context, eventMessages ...DomainEventMessage) (err error) {
	if eventMessages == nil {
		return
	}
	for _, eventMessage := range eventMessages {
		_localEventbusGroups.send(eventMessage.TopicName(bus.name), eventMessage)
	}
	return
}

func (bus *LocalEventBus) Recv(ctx context.Context, topic string, handle EventHandle) (err error) {

	if topic == "" {
		return
	}
	if handle == nil {
		return
	}
	bus.consumers[topic] = handle
	return
}

func (bus *LocalEventBus) Start(ctx context.Context) (err error) {

	go func(ctx context.Context, bus *LocalEventBus) {
		for {
			event, ok := <-bus.eventsCh
			if !ok {
				break
			}
			handle, has := bus.consumers[event.topic]
			if !has {
				continue
			}
			domainEvent := event.value.(DomainEvent)
			_, _ = handle(ctx, domainEvent)
			bus.eventCount.Done()
		}
	}(ctx, bus)
	return
}

func (bus *LocalEventBus) Shutdown() {
	close(bus.eventsCh)
	_localEventbusGroups.removeGroup(bus.name)
}

func (bus *LocalEventBus) Await() {

}

func (bus *LocalEventBus) Close(ctx context.Context) (err error) {
	bus.Shutdown()
	bus.Await()
	return
}
