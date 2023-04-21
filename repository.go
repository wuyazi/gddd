package gddd

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
)

var NodeNumber int64 = 1

type RepositorySaveListener interface {
	Handle(ctx context.Context, event DomainEventMessage)
}

type RepositoryConfig struct {
	DomainName                  string   `json:"domain_name"`
	SubDomainName               string   `json:"sub_domain_name"`
	MysqlEventStoreDBConfig     DBConfig `json:"mysql_event_store_db_config"`
	RocketMqEventBusNameServers []string `json:"rocket_mq_event_bus_name_servers"`
	SaveListener                RepositorySaveListener
}

func NewRepository(ctx context.Context, config *RepositoryConfig) (r *Repository, err error) {
	domainName := strings.TrimSpace(config.DomainName)
	if domainName == "" {
		panic("aggregates repository create failed, DomainName is empty")
	}
	subDomainName := strings.TrimSpace(config.SubDomainName)
	if subDomainName == "" {
		panic("aggregates repository create failed, SubDomainName is empty")
	}
	mysqlEventStoreConfig := MysqlEventStoreConfig{
		SubDomainName: config.SubDomainName,
		DBConfig:      config.MysqlEventStoreDBConfig,
	}
	es, esErr := NewMysqlEventStore(ctx, mysqlEventStoreConfig)
	if esErr != nil {
		panic(fmt.Errorf("aggregates repository create failed, new event store failed, err: %w", esErr))
	}
	dtmConfig := DtmEventProducerConfig{
		DomainName:    config.DomainName,
		SubDomainName: config.SubDomainName,
		NameServers:   config.RocketMqEventBusNameServers,
		EventStore:    &es,
	}
	eb, ebErr := NewDtmEventProducer(ctx, dtmConfig)
	if ebErr != nil {
		panic(fmt.Errorf("aggregates repository create failed, new dtm eventBus failed, err: %w", ebErr))
	}
	r = &Repository{
		es:           es,
		eb:           eb,
		saveListener: config.SaveListener,
	}
	return
}

type Repository struct {
	//noCopy       noCopy
	es           EventStore
	eb           DtmEventProducer
	saveListener RepositorySaveListener
}

func (r *Repository) SetSaveListener(ctx context.Context, saveListener RepositorySaveListener) (err error) {
	r.saveListener = saveListener
	return
}

func (r *Repository) RegisterAggregates(ctx context.Context, aggregates ...Aggregate) (err error) {
	for _, aggregate := range aggregates {
		aggregateName := getAggregateName(aggregate)
		r.es.InitDomainEventStoreTable(ctx, aggregateName)
	}
	return
}

func (r *Repository) Load(ctx context.Context, aggregate Aggregate) (has bool, err error) {
	aggregateId := aggregate.Identifier()
	if aggregateId == 0 {
		err = errors.New("aggregates repository load failed, aggregateId is empty")
		return
	}
	if aggregate == nil {
		err = errors.New("aggregates repository load failed, aggregate is nil")
		return
	}
	// load snapshot
	aggregateName := getAggregateName(aggregate)
	lastEventId, loadSnapshotErr := r.es.LoadSnapshot(ctx, aggregateId, aggregate)
	if loadSnapshotErr != nil {
		err = fmt.Errorf("aggregates repository load failed, load snapshot failed, aggregateName is %s, aggregateId is %d, %v", aggregateName, aggregateId, loadSnapshotErr)
		return
	}
	storedEvents, readEventsErr := r.es.ReadEvents(ctx, aggregateName, aggregateId, lastEventId)
	if readEventsErr != nil {
		err = fmt.Errorf("aggregates repository load failed, load events failed, aggregateName is %s, aggregateId is %d, %v", aggregateName, aggregateId, readEventsErr)
		return
	}
	if storedEvents == nil || len(storedEvents) == 0 {
		if lastEventId != 0 {
			has = true
			return
		}
		return
	}

	handleErr := handleStoredEventRecursively(aggregate, storedEvents)
	if handleErr != nil {
		err = fmt.Errorf("aggregates repository load failed, handle stored event recursively failed, aggregateName is %s, aggregateId is %d, %v", aggregateName, aggregateId, handleErr)
		return
	}

	has = true
	return
}

func (r *Repository) Save(ctx context.Context, aggregates ...Aggregate) (ok bool, err error) {
	if aggregates == nil || len(aggregates) == 0 {
		err = errors.New("aggregates repository save failed, aggregate is nil")
		return
	}

	eventsMessages := make([]DomainEventMessage, 0, 1)

	for _, aggregate := range aggregates {
		if aggregate == nil {
			err = errors.New("aggregates repository save failed, aggregate is nil")
			return
		}
		aggregateId := aggregate.Identifier()
		if aggregateId == 0 {
			err = errors.New("aggregates repository save failed, Identifier is empty")
			return
		}
		aggregateName := strings.TrimSpace(getAggregateName(aggregate))
		if aggregateName == "" {
			err = errors.New("aggregates repository save failed, AggregateName is empty")
			return
		}

		abstractAggregateField := reflect.ValueOf(aggregate).Elem().FieldByName("AbstractAggregate")
		if !abstractAggregateField.IsValid() {
			err = fmt.Errorf("aggregates repository save failed, check AbstractAggregateField failed, aggregateName is %s, aggregateId is %d", aggregateName, aggregateId)
			return
		}
		abstractAggregate, isAbstractAggregate := abstractAggregateField.Interface().(AbstractAggregate)
		if !isAbstractAggregate {
			err = fmt.Errorf("aggregates repository save failed, check AbstractAggregate failed, aggregateName is %s, aggregateId is %d", aggregateName, aggregateId)
			return
		}
		domainEvents := abstractAggregate.Applied()
		if domainEvents == nil || len(domainEvents) == 0 {
			err = fmt.Errorf("aggregates repository save failed, changes is empty, aggregateName is %s, aggregateId is %d", aggregateName, aggregateId)
			return
		}

		handleErr := handleAppliedDomainEventRecursively(aggregate, domainEvents)
		if handleErr != nil {
			err = fmt.Errorf("aggregates repository save failed, handle event recursively failed, aggregateName is %s, aggregateId is %d, %v", aggregateName, aggregateId, handleErr)
			return
		}
		for _, domainEvent := range domainEvents {
			publishEventsMessage, newMessageErr := newDomainEventMessage(domainEvent)
			if newMessageErr != nil {
				err = fmt.Errorf("aggregates repository save failed, new domain event message failed, aggregateName is %s, aggregateId is %d, eventId is %d, %v", aggregateName, aggregateId, newMessageErr, domainEvent.EventId())
				return
			}
			eventsMessages = append(eventsMessages, publishEventsMessage)
		}
		abstractAggregate.lifecycle.cleanDomainEvents()
		// send message
		sendErr := r.eb.Send(ctx, domainEvents...)
		if sendErr != nil {
			err = fmt.Errorf("aggregates repository save warn, send domain event failed, %v", sendErr)
			return
		}
	}

	ok = true

	if r.saveListener != nil {
		for _, eventMessage := range eventsMessages {
			r.saveListener.Handle(ctx, eventMessage)
		}
	}

	// snapshot
	for _, aggregate := range aggregates {
		makeSnapshotErr := r.es.MakeSnapshot(ctx, aggregate)
		if makeSnapshotErr != nil {
			// todo log it
			//err0 := fmt.Errorf("aggregates repository save warn, make snapshot failed, aggregateName is %s, aggregateId is %d, %v", getAggregateName(aggregate), aggregate.Identifier(), makeSnapshotErr)
		}
	}

	return
}
