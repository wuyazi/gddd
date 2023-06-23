package gddd

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/dtm-labs/client/dtmcli"
	"github.com/dtm-labs/client/dtmcli/dtmimp"
	"github.com/dtm-labs/client/dtmgrpc"
	"github.com/dtm-labs/dtmdriver"
	"github.com/lithammer/shortuuid/v3"
	"github.com/wuyazi/gddd/event_message"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"log"
	"runtime"
	"strings"
	"time"
)

type DtmEventProducerConfig struct {
	DomainName    string
	SubDomainName string
	NameServers   []string
	EventStore    *EventStore
	DtmDBConf     dtmimp.DBConf
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
	// start preparedB server
	app := Startup(config.DtmDBConf)
	time.Sleep(200 * time.Millisecond)
	go RunHTTP(app)
	//select {}
	return
}

func ExecuteLocalTransaction(ctx context.Context, es EventStore, eventsMessages []DomainEvent) error {
	storedEvents := make([]StoredEvent, 0, 1)
	for _, eventMessage := range eventsMessages {
		EventBodyRaw, err := eventMessage.EventBodyRaw()
		if err != nil {

		}
		storedEvent, err := newJsonStoredEvent(eventMessage.AggregateId(), eventMessage.AggregateName(), eventMessage.EventId(), eventMessage.EventName(), EventBodyRaw)
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

func (p *DtmEventProducer) Send(ctx context.Context, eventMessages ...DomainEvent) (err error) {
	if eventMessages == nil || len(eventMessages) == 0 {
		err = fmt.Errorf("dtm event producer send event failed, eventMessages is nil or empty")
		return
	}
	//dtmMsg := dtmcli.NewMsg("http://localhost:36789/api/dtmsvr", shortuuid.New())
	dtmdriver.Middlewares.Grpc = append(dtmdriver.Middlewares.Grpc, SetGrpcHeaderForDomainEvent)
	dtmMsg := dtmgrpc.NewMsgGrpc(p.Brokers[0], shortuuid.New())
	for _, eventMessage := range eventMessages {
		var messageBody []byte
		messageBody, err = json.Marshal(eventMessage)
		if err != nil {
			return
		}
		fmt.Errorf("%+v", messageBody)
		//dtmMsg = dtmMsg.Add("http://localhost:8081/api/busi/TransIn", &messageBody)
		abs := event_message.AbstractEvent{
			AggregateId:   eventMessage.AggregateId(),
			AggregateName: eventMessage.AggregateName(),
			EventId:       eventMessage.EventId(),
			EventName:     eventMessage.EventName(),
		}
		eventBody := proto.Clone(eventMessage.EventBody())
		fd := eventBody.ProtoReflect().Descriptor().Fields().ByName("abs")
		fv := protoreflect.ValueOfMessage(abs.ProtoReflect())
		eventBody.ProtoReflect().Set(fd, fv)
		dtmMsg.AddTopic("create_user", eventBody)
	}
	dtmMsg.BranchHeaders = map[string]string{"test_header": "aaasdfa"}
	dtmcli.SetBarrierTableName("myhubdb.barrier")
	err = dtmMsg.DoAndSubmitDB(fmt.Sprintf("http://localhost:%d/api/busi/QueryPreparedB", BusiPort), p.EventStore.GetDB(ctx),
		func(tx *sql.Tx) error {
			// TODO use tx
			ctx = context.WithValue(ctx, "tx", tx)
			return ExecuteLocalTransaction(ctx, p.EventStore, eventMessages)
		})
	if err != nil {
		return err
	}
	return
}

// SetGrpcHeaderForDomainEvent interceptor to set head for DomainEvent
func SetGrpcHeaderForDomainEvent(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	md := metadata.New(map[string]string{"test_header": "test"})
	ctx = metadata.NewOutgoingContext(ctx, md)
	return invoker(ctx, method, req, reply, cc, opts...)
}

func UnaryClientInterceptor() grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply interface{},
		cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		// 预处理(pre-processing)
		start := time.Now()
		// 获取正在运行程序的操作系统
		cos := runtime.GOOS
		// 将操作系统信息附加到传出请求
		ctx = metadata.AppendToOutgoingContext(ctx, "client-os", cos)

		// 可以看做是当前 RPC 方法，一般在拦截器中调用 invoker 能达到调用 RPC 方法的效果，当然底层也是 gRPC 在处理。
		// 调用RPC方法(invoking RPC method)
		err := invoker(ctx, method, req, reply, cc, opts...)

		// 后处理(post-processing)
		end := time.Now()
		log.Printf("RPC: %s,,client-OS: '%v' req:%v start time: %s, end time: %s, err: %v", method, cos, req, start.Format(time.RFC3339), end.Format(time.RFC3339), err)
		return err
	}
}

type DtmEventConsumerConfig struct {
	DomainName  string
	GroupName   string
	NameServers []string
}
