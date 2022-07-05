package gddd

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/valyala/bytebufferpool"
	"reflect"
	"strings"
)

type DBConfig struct {
	SqlDataSourceName  string `json:"sql_data_source_name"`
	MaxIdleConnections int    `json:"max_idle_connections"`
	MaxOpenConnections int    `json:"max_open_connections"`
}

type MysqlEventStoreConfig struct {
	SubDomainName string
	DBConfig      DBConfig
}

// for example: users_location_domain_events
func getDomainTableName(subDomainName string, aggregateName string, tableType string) string {
	return fmt.Sprintf("%s_%s_domain_%s", strings.ToLower(subDomainName),
		strings.TrimSuffix(strings.ToLower(aggregateName), "aggregate"), tableType)
}

const (
	// create table
	sqlCreateDomainEventTable = `create table if not exists %s (
		id bigint unsigned auto_increment primary key,
		aggregate_id bigint not null,
		event_id bigint not null,
		event_name varchar(50) not null,
		event_data text not null,
		unique index ix_aggregate_id (aggregate_id),
		unique index ix_event_id (event_id)
	)charset 'utf8mb4';`
	sqlCreateDomainSnapshotTable = `create table if not exists %s (
		aggregate_id bigint not null primary key,
		last_event_id bigint not null,
		snapshot_data text not null
	)charset 'utf8mb4';`
	// events
	sqlInsertEventPrefix                         = "INSERT INTO %s (aggregate_id, event_id, event_name, event_data) VALUES "
	sqlSelectEventsByAggregateId                 = "SELECT event_id, event_name, event_data FROM %s WHERE aggregate_id = ? ORDER BY id ASC"
	sqlSelectEventsByAggregateIdAndEventId       = "SELECT event_id, event_name, event_data FROM %s WHERE aggregate_id = ? AND id > (SELECT id FROM %s where event_id = ?) ORDER BY id ASC"
	sqlGetLatestEventRowIdByAggregateId          = "SELECT id FROM %s WHERE aggregate_id = ? ORDER BY id DESC LIMIT 1 OFFSET 0"
	sqlGetEventRowIdByEventId                    = "SELECT id FROM %s WHERE event_id = ?"
	sqlCountEventsByAggregateIdAndEventsIdRange  = "SELECT count(id) FROM %s WHERE aggregate_id = ? and id > ? and id <= ? ORDER BY id ASC"
	sqlSelectEventsByAggregateIdAndEventsIdRange = "SELECT event_id, event_name, event_data FROM %s WHERE aggregate_id = ? and id > ? and id <= ? ORDER BY id ASC"
	sqlGetLastEventIdByAggregateId               = "SELECT last_event_id FROM %s WHERE aggregate_id = ?"
	// snapshot
	sqlInsertOrUpdateSnapshot       = "INSERT INTO %s (aggregate_id, last_event_id, snapshot_data) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE snapshot_data = ?"
	sqlGetSnapshotDataByAggregateId = "SELECT last_event_id, snapshot_data FROM %s WHERE aggregate_id = ?"
)

func NewMysqlEventStore(ctx context.Context, config MysqlEventStoreConfig) (es EventStore, err error) {
	subDomainName := strings.TrimSpace(config.SubDomainName)
	if subDomainName == "" {
		err = errors.New("new mysql event store failed, SubDomainName is empty")
		return
	}
	sqlDataSourceName := strings.TrimSpace(config.DBConfig.SqlDataSourceName)
	if sqlDataSourceName == "" {
		err = errors.New("new mysql event store failed, SqlDataSourceName is empty")
		return
	}
	db, openDBErr := sql.Open("mysql", config.DBConfig.SqlDataSourceName)
	if openDBErr != nil {
		err = fmt.Errorf("new mysql event store failed, open mysql db failed, %w", openDBErr)
		return
	}
	es0 := &mysqlEventStore{
		//noCopy:                        noCopy{},
		subDomainName: subDomainName,
		db:            db,
		snapshotsChan: make(chan Aggregate, 1024),
	}
	es0.listenSnapshotsChan(ctx)
	es = es0
	return
}

type mysqlEventStore struct {
	//noCopy                        noCopy
	subDomainName string
	db            *sql.DB
	snapshotsChan chan Aggregate
}

func (es *mysqlEventStore) listenSnapshotsChan(ctx context.Context) {
	go func(ctx context.Context, es *mysqlEventStore) {
		stopped := false
		for {
			select {
			case <-ctx.Done():
				stopped = true
				break
			case snapshot, ok := <-es.snapshotsChan:
				if !ok {
					stopped = true
					break
				}
				_ = es.doMakeSnapshot(ctx, snapshot)
			}
			if stopped {
				break
			}
		}
	}(ctx, es)
	return
}

func (es *mysqlEventStore) Name() (name string) {
	name = es.subDomainName
	return
}

func (es *mysqlEventStore) InitDomainEventStoreTable(ctx context.Context, aggregateName string) {
	domainEventTableName := getDomainTableName(es.subDomainName, aggregateName, "events")
	createEventSql := fmt.Sprintf(sqlCreateDomainEventTable, domainEventTableName)
	_, createEventTableErr := es.db.ExecContext(ctx, createEventSql)
	if createEventTableErr != nil {
		err := fmt.Errorf("mysql event store (%s) create event table failed, aggregateName is %s, %v", es.subDomainName, aggregateName, createEventTableErr)
		panic(err)
	}
	domainSnapshotTableName := getDomainTableName(es.subDomainName, aggregateName, "snapshot")
	createSnapshotSql := fmt.Sprintf(sqlCreateDomainSnapshotTable, domainSnapshotTableName)
	_, createSnapshotTableErr := es.db.ExecContext(ctx, createSnapshotSql)
	if createSnapshotTableErr != nil {
		err := fmt.Errorf("mysql event store (%s) create snapshot table failed, aggregateName is %s, %v", es.subDomainName, aggregateName, createSnapshotTableErr)
		panic(err)
	}
	return
}

func (es *mysqlEventStore) ReadEvents(ctx context.Context, aggregateName string, aggregateId int64, lastEventId int64) (events []StoredEvent, err error) {
	if aggregateId == 0 {
		err = fmt.Errorf("mysql event store (%s) read events failed, aggregateId is empty", es.subDomainName)
		return
	}
	domainEventTableName := getDomainTableName(es.subDomainName, aggregateName, "events")
	var listSQL string
	listArgs := make([]interface{}, 0, 1)
	if lastEventId == 0 {
		listSQL = fmt.Sprintf(sqlSelectEventsByAggregateId, domainEventTableName)
		listArgs = append(listArgs, aggregateId)
	} else {
		listSQL = fmt.Sprintf(sqlSelectEventsByAggregateIdAndEventId, domainEventTableName, domainEventTableName)
		listArgs = append(listArgs, aggregateId, lastEventId)
	}
	rows, queryErr := es.db.QueryContext(ctx, listSQL, listArgs...)
	if queryErr != nil {
		err = fmt.Errorf("mysql event store (%s) read events failed, query failed, aggregateName is %s, aggregateId is %d, lastEventId is %d, %v", es.subDomainName, aggregateName, aggregateId, lastEventId, queryErr)
		return
	}

	events = make([]StoredEvent, 0, 1)
	for rows.Next() {
		var eventId int64
		var eventName string
		var eventData = make([]byte, 0, 1)
		scanErr := rows.Scan(
			&eventId,
			&eventName,
			&eventData,
		)
		if scanErr != nil {
			_ = rows.Close()
			err = fmt.Errorf("mysql event store (%s) read events failed, query failed at scan, aggregateName is %s, aggregateId is %d, lastEventId is %d, %v", es.subDomainName, aggregateName, aggregateId, lastEventId, scanErr)
			return
		}
		var event StoredEvent
		event, err = newJsonStoredEvent(aggregateId, aggregateName, eventId, eventName, eventData)
		if err != nil {
			return
		}
		events = append(events, event)
	}
	closeErr := rows.Close()
	if closeErr != nil {
		err = fmt.Errorf("mysql event store (%s) read events failed, close failed, aggregateName is %s, aggregateId is %d, lastEventId is %d, %v", es.subDomainName, aggregateName, aggregateId, lastEventId, closeErr)
		return
	}
	return
}

func (es *mysqlEventStore) StoreEvents(ctx context.Context, events []StoredEvent) (err error) {
	if events == nil || len(events) == 0 {
		err = fmt.Errorf("mysql event store (%s) store events failed, events is nil or empty", es.subDomainName)
		return
	}
	sqlStmtMap := make(map[string][]interface{})
	for _, event := range events {
		aggregateName := event.AggregateName()
		if _, ok := sqlStmtMap[aggregateName]; !ok {
			sqlStmtMap[aggregateName] = make([]interface{}, 0, 1)
		}
		sqlStmtMap[aggregateName] = append(sqlStmtMap[aggregateName],
			event.AggregateId(),
			event.EventId(),
			event.EventName(),
			event.EventBodyRaw(),
		)
	}
	tx, txErr := es.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted, ReadOnly: false})
	if txErr != nil {
		err = fmt.Errorf("mysql event store (%s) store events failed, begin database tx failed, %v", es.subDomainName, txErr)
		return
	}
	for aggregateName, sqlStmtArgs := range sqlStmtMap {
		insertSQLBuffer := bytebufferpool.Get()
		domainEventTableName := getDomainTableName(es.subDomainName, aggregateName, "events")
		_, _ = insertSQLBuffer.WriteString(fmt.Sprintf(sqlInsertEventPrefix, domainEventTableName))
		_, _ = insertSQLBuffer.WriteString(strings.Repeat("(?, ?, ?, ?), ", len(sqlStmtArgs)/4))
		insertSQL := strings.TrimSuffix(insertSQLBuffer.String(), ", ")
		rs, insertErr := tx.ExecContext(ctx, insertSQL, sqlStmtArgs...)
		if insertErr != nil {
			_ = tx.Rollback()
			err = fmt.Errorf("mysql event store (%s) store events failed, insert failed, aggregateName is %s, %v", es.subDomainName, aggregateName, insertErr)
			return
		}
		affected, affectedErr := rs.RowsAffected()
		if affectedErr != nil {
			_ = tx.Rollback()
			err = fmt.Errorf("mysql event store (%s) store events failed, insert failed. aggregateName is %s, get rows affected failed, %v", es.subDomainName, aggregateName, affectedErr)
			return
		}
		if affected == 0 {
			_ = tx.Rollback()
			err = fmt.Errorf("mysql event store (%s) store events failed, insert failed. aggregateName is %s, no rows affected failed, affected = %v", es.subDomainName, aggregateName, affected)
			return
		}
	}
	cmtErr := tx.Commit()
	if cmtErr != nil {
		_ = tx.Rollback()
		err = fmt.Errorf("mysql event store (%s) store events failed, insert failed. commit failed, %v", es.subDomainName, cmtErr)
		return
	}
	return
}

func (es *mysqlEventStore) CheckEvents(ctx context.Context, events []StoredEvent) (err error) {
	if events == nil {
		err = fmt.Errorf("mysql event store (%s) check events failed, events is nil", es.subDomainName)
		return
	}
	for _, event := range events {
		aggregateName := event.AggregateName()
		var rowId int64
		rowId, err = es.getEventRowId(ctx, aggregateName, event.EventId())
		if err != nil {
			err = fmt.Errorf("mysql event store (%s) check events failed, get rowId failed, aggregateName is %s", es.subDomainName, aggregateName)
			return
		}
		if rowId == 0 {
			err = fmt.Errorf("mysql event store (%s) check events failed, get rowId failed, rowId is 0, aggregateName is %s", es.subDomainName, aggregateName)
			return
		}
	}
	return
}

func (es *mysqlEventStore) getEventRowId(ctx context.Context, aggregateName string, eventId int64) (rowId int64, err error) {
	domainEventTableName := getDomainTableName(es.subDomainName, aggregateName, "events")
	row, queryErr := es.db.QueryContext(ctx, fmt.Sprintf(sqlGetEventRowIdByEventId, domainEventTableName), &eventId)
	if queryErr != nil {
		err = fmt.Errorf("mysql event store (%s) make snapshot failed, load row id from %s failed, eventId is %d, %v", es.subDomainName, domainEventTableName, eventId, queryErr)
		return
	}
	if row.Next() {
		eventRowId := sql.NullInt64{}
		scanErr := row.Scan(&eventRowId)
		if scanErr != nil {
			_ = row.Close()
			err = fmt.Errorf("mysql event store (%s) make snapshot failed, load row id from %s failed at scan, eventId is %d, %v", es.subDomainName, domainEventTableName, eventId, scanErr)
			return
		}
		rowId = eventRowId.Int64
	}
	closeErr := row.Close()
	if closeErr != nil {
		err = fmt.Errorf("mysql event store (%s) make snapshot failed, close failed, eventId is %d, %v", es.subDomainName, eventId, closeErr)
		return
	}
	return
}

func (es *mysqlEventStore) getLatestEventRowIdOfAggregate(ctx context.Context, aggregateName string, aggregateId int64) (rowId int64, err error) {
	domainEventTableName := getDomainTableName(es.subDomainName, aggregateName, "events")
	row, queryErr := es.db.QueryContext(ctx, fmt.Sprintf(sqlGetLatestEventRowIdByAggregateId, domainEventTableName), &aggregateId)
	if queryErr != nil {
		err = fmt.Errorf("mysql event store (%s) make snapshot failed, load latest event id of aggregate from %s failed, aggregateName is %s, aggregateId is %d, %v", es.subDomainName, domainEventTableName, aggregateName, aggregateId, queryErr)
		return
	}
	if row.Next() {
		eventRowId := sql.NullInt64{}
		scanErr := row.Scan(&eventRowId)
		if scanErr != nil {
			_ = row.Close()
			err = fmt.Errorf("mysql event store (%s) make snapshot failed, load latest event id of aggregate from %s failed at scan, aggregateName is %s, aggregateId is %d, %v", es.subDomainName, domainEventTableName, aggregateName, aggregateId, scanErr)
			return
		}
		rowId = eventRowId.Int64
	}
	closeErr := row.Close()
	if closeErr != nil {
		err = fmt.Errorf("mysql event store (%s) make snapshot failed, close failed, aggregateName is %s, aggregateId is %d, %v", es.subDomainName, aggregateName, aggregateId, closeErr)
		return
	}
	return
}

// list >leftRowId, <=rightRowId stored events
func (es *mysqlEventStore) countStoredEvents(ctx context.Context, aggregateName string, aggregateId int64, leftRowId int64, rightRowId int64) (count int64, err error) {
	if aggregateId == 0 {
		err = fmt.Errorf("mysql event store (%s) count stored events failed, aggregateName is %s, aggregateId is empty", es.subDomainName, aggregateName)
		return
	}
	domainEventTableName := getDomainTableName(es.subDomainName, aggregateName, "events")
	listSQL := fmt.Sprintf(sqlCountEventsByAggregateIdAndEventsIdRange, domainEventTableName)
	rows, queryErr := es.db.QueryContext(ctx, listSQL, aggregateId, leftRowId, rightRowId)
	if queryErr != nil {
		err = fmt.Errorf("mysql event store (%s) count stored events failed, query failed, aggregateName is %s, aggregateId is %d, leftRowId is %d, rightRowId is %d, %v", es.subDomainName, aggregateName, aggregateId, leftRowId, rightRowId, queryErr)
		return
	}

	for rows.Next() {
		count0 := sql.NullInt64{}
		scanErr := rows.Scan(
			&count0,
		)
		if scanErr != nil {
			_ = rows.Close()
			err = fmt.Errorf("mysql event store (%s) count stored events failed, query failed at scan, aggregateName is %s, aggregateId is %d, leftRowId is %d, rightRowId is %d, %v", es.subDomainName, aggregateName, aggregateId, leftRowId, rightRowId, scanErr)
			return
		}
		count = count0.Int64
	}
	closeErr := rows.Close()
	if closeErr != nil {
		err = fmt.Errorf("mysql event store (%s) count stored events failed, close failed, aggregateName is %s, aggregateId is %d, leftRowId is %d, rightRowId is %d, %v", es.subDomainName, aggregateName, aggregateId, leftRowId, rightRowId, closeErr)
		return
	}
	return
}

// list >leftRowId, <=rightRowId stored events
func (es *mysqlEventStore) rangeStoredEvents(ctx context.Context, aggregateName string, aggregateId int64, leftRowId int64, rightRowId int64) (events []StoredEvent, err error) {
	if aggregateId == 0 {
		err = fmt.Errorf("mysql event store (%s) range stored events failed, aggregateName is %s, aggregateId is empty", es.subDomainName, aggregateName)
		return
	}
	domainEventTableName := getDomainTableName(es.subDomainName, aggregateName, "events")
	listSQL := fmt.Sprintf(sqlSelectEventsByAggregateIdAndEventsIdRange, domainEventTableName)
	rows, queryErr := es.db.QueryContext(ctx, listSQL, aggregateId, leftRowId, rightRowId)
	if queryErr != nil {
		err = fmt.Errorf("mysql event store (%s) range stored events failed, query failed, aggregateName is %s, aggregateId is %d, leftRowId is %d, rightRowId is %d, %v", es.subDomainName, aggregateName, aggregateId, leftRowId, rightRowId, queryErr)
		return
	}

	events = make([]StoredEvent, 0, 1)
	for rows.Next() {
		eventId := sql.NullInt64{}
		eventName := sql.NullString{}
		eventData := make([]byte, 0, 1)
		scanErr := rows.Scan(
			&eventId,
			&eventName,
			&eventData,
		)
		if scanErr != nil {
			_ = rows.Close()
			err = fmt.Errorf("mysql event store (%s) range stored events failed, query failed at scan, aggregateName is %s, aggregateId is %d, leftRowId is %d, rightRowId is %d, %v", es.subDomainName, aggregateName, aggregateId, leftRowId, rightRowId, scanErr)
			return
		}
		var event StoredEvent
		event, err = newJsonStoredEvent(aggregateId, aggregateName, eventId.Int64, eventName.String, eventData)
		if err != nil {
			return
		}
		events = append(events, event)
	}
	closeErr := rows.Close()
	if closeErr != nil {
		err = fmt.Errorf("mysql event store (%s) range stored events failed, close failed, aggregateName is %s, aggregateId is %d, leftRowId is %d, rightRowId is %d, %v", es.subDomainName, aggregateName, aggregateId, leftRowId, rightRowId, closeErr)
		return
	}
	return
}

func (es *mysqlEventStore) MakeSnapshot(_ context.Context, aggregate Aggregate) (err error) {
	if aggregate == nil {
		err = fmt.Errorf("mysql event store (%s) make snapshot failed, aggregate is nil", es.subDomainName)
		return
	}
	aggregateId := aggregate.Identifier()
	if aggregateId == 0 {
		err = fmt.Errorf("mysql event store (%s) make snapshot failed, aggregateId is nil", es.subDomainName)
		return
	}
	es.snapshotsChan <- aggregate
	return
}

func (es *mysqlEventStore) doMakeSnapshot(ctx context.Context, aggregate Aggregate) (err error) {
	if aggregate == nil {
		err = fmt.Errorf("mysql event store (%s) make snapshot failed, aggregate is nil", es.subDomainName)
		return
	}
	aggregateId := aggregate.Identifier()
	if aggregateId == 0 {
		err = fmt.Errorf("mysql event store (%s) make snapshot failed, aggregateId is nil", es.subDomainName)
		return
	}

	// todo
	emptyAggregate0 := reflect.New(reflect.TypeOf(aggregate).Elem())
	emptyAggregateInterface := emptyAggregate0.Interface()
	emptyAggregate, isAggregate := emptyAggregateInterface.(Aggregate)
	if !isAggregate {
		err = fmt.Errorf("mysql event store (%s) make snapshot failed, aggregate is not aggregate, %v", es.subDomainName, aggregate)
		return
	}

	aggregateName := getAggregateName(emptyAggregate)
	prevEventId, getPrevEventIdErr := es.LoadSnapshot(ctx, aggregateId, emptyAggregate)
	if getPrevEventIdErr != nil {
		err = fmt.Errorf("mysql event store (%s) make snapshot failed, aggregateName is %s, aggregateId is %d, %v", es.subDomainName, aggregateName, aggregateId, getPrevEventIdErr)
		return
	}
	prevEventRowId := int64(0)
	if prevEventId != 0 {
		prevEventRowId0, getPrevEventRowIdErr := es.getEventRowId(ctx, aggregateName, prevEventId)
		if getPrevEventRowIdErr != nil {
			err = fmt.Errorf("mysql event store (%s) make snapshot failed, aggregateName is %s, aggregateId is %d, %v", es.subDomainName, aggregateName, aggregateId, getPrevEventRowIdErr)
			return
		}
		prevEventRowId = prevEventRowId0
	} else {
		emptyAggregate = aggregate
	}
	// count of not be made snapshot events
	latestEventRowId, getLatestEventIdErr := es.getLatestEventRowIdOfAggregate(ctx, aggregateName, aggregateId)
	if getLatestEventIdErr != nil {
		err = fmt.Errorf("mysql event store (%s) make snapshot failed, aggregateName is %s, aggregateId is %d, %v", es.subDomainName, aggregateName, aggregateId, getLatestEventIdErr)
		return
	}
	count, countErr := es.countStoredEvents(ctx, aggregateName, aggregateId, prevEventRowId, latestEventRowId)
	if countErr != nil {
		err = fmt.Errorf("mysql event store (%s) make snapshot failed, aggregateName is %s, aggregateId is %d, %v", es.subDomainName, aggregateName, aggregateId, countErr)
		return
	}

	makeFlag := count > 10
	if !makeFlag {
		return
	}

	// make
	// get not be made events (prevEventId:latestEventId]
	storedEvents, rangeStoredEventsErr := es.rangeStoredEvents(ctx, aggregateName, aggregateId, prevEventRowId, latestEventRowId)
	if rangeStoredEventsErr != nil {
		err = fmt.Errorf("mysql event store (%s) make snapshot failed, aggregateName is %s, aggregateId is %d, %v", es.subDomainName, aggregateName, aggregateId, rangeStoredEventsErr)
		return
	}
	if storedEvents == nil || len(storedEvents) == 0 {
		return
	}

	lastEventId := storedEvents[len(storedEvents)-1].EventId()
	handleErr := handleStoredEventRecursively(emptyAggregate, storedEvents)
	if handleErr != nil {
		err = fmt.Errorf("mysql event store (%s) make snapshot failed, handle stored event recursively falied, aggregateName is %s, aggregateId is %d, %v", es.subDomainName, aggregateName, aggregateId, handleErr)
		return
	}

	aggregateBytes, encodeErr := json.Marshal(emptyAggregate)
	if encodeErr != nil {
		err = fmt.Errorf("mysql event store (%s) make snapshot failed, encode aggregate failed, aggregateName is %s, aggregateId is %d, lastEventId is %d, %v", es.subDomainName, aggregateName, aggregateId, lastEventId, encodeErr)
		return
	}
	domainSnapshotTableName := getDomainTableName(es.subDomainName, aggregateName, "snapshot")
	insertSQL := fmt.Sprintf(sqlInsertOrUpdateSnapshot, domainSnapshotTableName)
	tx, txErr := es.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted, ReadOnly: false})
	if txErr != nil {
		err = fmt.Errorf("mysql event store (%s) make snapshot failed, begin database tx failed, aggregateName is %s, aggregateId is %d, lastEventId is %d, %v", es.subDomainName, aggregateName, aggregateId, lastEventId, txErr)
		return
	}
	rs, insertErr := tx.ExecContext(ctx, insertSQL, aggregateId, lastEventId, aggregateBytes, aggregateBytes)
	if insertErr != nil {
		_ = tx.Rollback()
		err = fmt.Errorf("mysql event store (%s) make snapshot failed, insert failed, aggregateName is %s, aggregateId is %d, lastEventId is %d, %v", es.subDomainName, aggregateName, aggregateId, lastEventId, insertErr)
		return
	}
	affected, affectedErr := rs.RowsAffected()
	if affectedErr != nil {
		_ = tx.Rollback()
		err = fmt.Errorf("mysql event store (%s) make snapshot failed, insert failed, aggregateName is %s, aggregateId is %d, lastEventId is %d. get rows affected failed, %v", es.subDomainName, aggregateName, aggregateId, lastEventId, affectedErr)
		return
	}
	if affected == 0 {
		_ = tx.Rollback()
		err = fmt.Errorf("mysql event store (%s) make snapshot failed, insert failed, aggregateName is %s, aggregateId is %d, lastEventId is %d. no rows affected failed, affected = %v", es.subDomainName, aggregateName, aggregateId, lastEventId, affected)
		return
	}
	cmtErr := tx.Commit()
	if cmtErr != nil {
		_ = tx.Rollback()
		err = fmt.Errorf("mysql event store (%s) make snapshot failed, insert failed, aggregateName is %s, aggregateId is %d, lastEventId is %d. commit failed, %v", es.subDomainName, aggregateName, aggregateId, lastEventId, cmtErr)
		return
	}
	return
}

func (es *mysqlEventStore) getLastEventIdFromSnapshot(ctx context.Context, aggregateName string, aggregateId int64) (lastEventId int64, err error) {
	if aggregateId == 0 {
		err = fmt.Errorf("mysql event store (%s) get last event id from snapshot failed, aggregateId is empty", es.subDomainName)
		return
	}
	domainSnapshotTableName := getDomainTableName(es.subDomainName, aggregateName, "snapshot")
	getSQL := fmt.Sprintf(sqlGetLastEventIdByAggregateId, domainSnapshotTableName)
	rows, queryErr := es.db.QueryContext(ctx, getSQL, &aggregateId)
	if queryErr != nil {
		err = fmt.Errorf("mysql event store (%s) get last event id from snapshot failed, query failed, aggregateName is %s, aggregateId is %d, %v", es.subDomainName, aggregateName, aggregateId, queryErr)
		return
	}
	if rows.Next() {
		lastEventId0 := sql.NullInt64{}
		scanErr := rows.Scan(
			&lastEventId0,
		)
		if scanErr != nil {
			_ = rows.Close()
			err = fmt.Errorf("mysql event store (%s) get last event id from snapshot failed, query failed at scan, aggregateName is %s, aggregateId is %d, %v", es.subDomainName, aggregateName, aggregateId, scanErr)
			return
		}
		lastEventId = lastEventId0.Int64
	}
	closeErr := rows.Close()
	if closeErr != nil {
		err = fmt.Errorf("mysql event store (%s) get last event id from snapshot failed, close failed, aggregateName is %s, aggregateId is %d, %v", es.subDomainName, aggregateName, aggregateId, closeErr)
		return
	}
	return
}

func (es *mysqlEventStore) LoadSnapshot(ctx context.Context, aggregateId int64, emptyAggregate Aggregate) (lastEventId int64, err error) {
	if aggregateId == 0 {
		err = fmt.Errorf("mysql event store (%s) load snapshot failed, aggregateId is empty", es.subDomainName)
		return
	}
	if emptyAggregate == nil {
		err = fmt.Errorf("mysql event store (%s) load snapshot failed, aggregate is nil", es.subDomainName)
		return
	}
	aggregateName := getAggregateName(emptyAggregate)
	domainSnapshotTableName := getDomainTableName(es.subDomainName, aggregateName, "snapshot")
	getSQL := fmt.Sprintf(sqlGetSnapshotDataByAggregateId, domainSnapshotTableName)
	rows, queryErr := es.db.QueryContext(ctx, getSQL, &aggregateId)
	if queryErr != nil {
		err = fmt.Errorf("mysql event store (%s) load snapshot failed, query failed, aggregateName is %s, aggregateId is %d, %v", es.subDomainName, aggregateName, aggregateId, queryErr)
		return
	}

	snapshotData := make([]byte, 0, 1)
	if rows.Next() {
		lastEventId0 := sql.NullInt64{}
		scanErr := rows.Scan(
			&lastEventId0,
			&snapshotData,
		)
		if scanErr != nil {
			_ = rows.Close()
			err = fmt.Errorf("mysql event store (%s) load snapshot failed, query failed at scan, aggregateName is %s, aggregateId is %d, %v", es.subDomainName, aggregateName, aggregateId, scanErr)
			return
		}
		lastEventId = lastEventId0.Int64
	}
	closeErr := rows.Close()
	if closeErr != nil {
		err = fmt.Errorf("mysql event store (%s) load snapshot failed, close failed, aggregateName is %s, aggregateId is %d, %v", es.subDomainName, aggregateName, aggregateId, closeErr)
		return
	}

	if lastEventId == 0 {
		return
	}
	decodeErr := json.Unmarshal(snapshotData, emptyAggregate)
	if decodeErr != nil {
		err = fmt.Errorf("mysql event store (%s) load snapshot failed, decode failed, aggregateName is %s, aggregateId is %d, %v", es.subDomainName, aggregateName, aggregateId, decodeErr)
		return
	}
	return
}
