// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.30.0
// 	protoc        v4.22.3
// source: message.proto

package proto

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type DomainEventMessageProto struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	AggregateId   int64  `protobuf:"varint,1,opt,name=aggregateId,proto3" json:"aggregateId,omitempty"`
	AggregateName string `protobuf:"bytes,2,opt,name=aggregateName,proto3" json:"aggregateName,omitempty"`
	EventName     string `protobuf:"bytes,3,opt,name=eventName,proto3" json:"eventName,omitempty"`
	EventId       int64  `protobuf:"varint,4,opt,name=eventId,proto3" json:"eventId,omitempty"`
	EventBody     []byte `protobuf:"bytes,5,opt,name=EventBody,proto3" json:"EventBody,omitempty"`
}

func (x *DomainEventMessageProto) Reset() {
	*x = DomainEventMessageProto{}
	if protoimpl.UnsafeEnabled {
		mi := &file_message_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *DomainEventMessageProto) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*DomainEventMessageProto) ProtoMessage() {}

func (x *DomainEventMessageProto) ProtoReflect() protoreflect.Message {
	mi := &file_message_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use DomainEventMessageProto.ProtoReflect.Descriptor instead.
func (*DomainEventMessageProto) Descriptor() ([]byte, []int) {
	return file_message_proto_rawDescGZIP(), []int{0}
}

func (x *DomainEventMessageProto) GetAggregateId() int64 {
	if x != nil {
		return x.AggregateId
	}
	return 0
}

func (x *DomainEventMessageProto) GetAggregateName() string {
	if x != nil {
		return x.AggregateName
	}
	return ""
}

func (x *DomainEventMessageProto) GetEventName() string {
	if x != nil {
		return x.EventName
	}
	return ""
}

func (x *DomainEventMessageProto) GetEventId() int64 {
	if x != nil {
		return x.EventId
	}
	return 0
}

func (x *DomainEventMessageProto) GetEventBody() []byte {
	if x != nil {
		return x.EventBody
	}
	return nil
}

type AbstractEvent struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	AggregateId   int64  `protobuf:"varint,1,opt,name=aggregateId,proto3" json:"aggregateId,omitempty"`
	AggregateName string `protobuf:"bytes,2,opt,name=aggregateName,proto3" json:"aggregateName,omitempty"`
	EventId       int64  `protobuf:"varint,3,opt,name=eventId,proto3" json:"eventId,omitempty"`
	EventName     string `protobuf:"bytes,4,opt,name=eventName,proto3" json:"eventName,omitempty"`
}

func (x *AbstractEvent) Reset() {
	*x = AbstractEvent{}
	if protoimpl.UnsafeEnabled {
		mi := &file_message_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *AbstractEvent) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*AbstractEvent) ProtoMessage() {}

func (x *AbstractEvent) ProtoReflect() protoreflect.Message {
	mi := &file_message_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use AbstractEvent.ProtoReflect.Descriptor instead.
func (*AbstractEvent) Descriptor() ([]byte, []int) {
	return file_message_proto_rawDescGZIP(), []int{1}
}

func (x *AbstractEvent) GetAggregateId() int64 {
	if x != nil {
		return x.AggregateId
	}
	return 0
}

func (x *AbstractEvent) GetAggregateName() string {
	if x != nil {
		return x.AggregateName
	}
	return ""
}

func (x *AbstractEvent) GetEventId() int64 {
	if x != nil {
		return x.EventId
	}
	return 0
}

func (x *AbstractEvent) GetEventName() string {
	if x != nil {
		return x.EventName
	}
	return ""
}

var File_message_proto protoreflect.FileDescriptor

var file_message_proto_rawDesc = []byte{
	0x0a, 0x0d, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12,
	0x05, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x22, 0xb7, 0x01, 0x0a, 0x17, 0x44, 0x6f, 0x6d, 0x61, 0x69,
	0x6e, 0x45, 0x76, 0x65, 0x6e, 0x74, 0x4d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x50, 0x72, 0x6f,
	0x74, 0x6f, 0x12, 0x20, 0x0a, 0x0b, 0x61, 0x67, 0x67, 0x72, 0x65, 0x67, 0x61, 0x74, 0x65, 0x49,
	0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x03, 0x52, 0x0b, 0x61, 0x67, 0x67, 0x72, 0x65, 0x67, 0x61,
	0x74, 0x65, 0x49, 0x64, 0x12, 0x24, 0x0a, 0x0d, 0x61, 0x67, 0x67, 0x72, 0x65, 0x67, 0x61, 0x74,
	0x65, 0x4e, 0x61, 0x6d, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0d, 0x61, 0x67, 0x67,
	0x72, 0x65, 0x67, 0x61, 0x74, 0x65, 0x4e, 0x61, 0x6d, 0x65, 0x12, 0x1c, 0x0a, 0x09, 0x65, 0x76,
	0x65, 0x6e, 0x74, 0x4e, 0x61, 0x6d, 0x65, 0x18, 0x03, 0x20, 0x01, 0x28, 0x09, 0x52, 0x09, 0x65,
	0x76, 0x65, 0x6e, 0x74, 0x4e, 0x61, 0x6d, 0x65, 0x12, 0x18, 0x0a, 0x07, 0x65, 0x76, 0x65, 0x6e,
	0x74, 0x49, 0x64, 0x18, 0x04, 0x20, 0x01, 0x28, 0x03, 0x52, 0x07, 0x65, 0x76, 0x65, 0x6e, 0x74,
	0x49, 0x64, 0x12, 0x1c, 0x0a, 0x09, 0x45, 0x76, 0x65, 0x6e, 0x74, 0x42, 0x6f, 0x64, 0x79, 0x18,
	0x05, 0x20, 0x01, 0x28, 0x0c, 0x52, 0x09, 0x45, 0x76, 0x65, 0x6e, 0x74, 0x42, 0x6f, 0x64, 0x79,
	0x22, 0x8f, 0x01, 0x0a, 0x0d, 0x41, 0x62, 0x73, 0x74, 0x72, 0x61, 0x63, 0x74, 0x45, 0x76, 0x65,
	0x6e, 0x74, 0x12, 0x20, 0x0a, 0x0b, 0x61, 0x67, 0x67, 0x72, 0x65, 0x67, 0x61, 0x74, 0x65, 0x49,
	0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x03, 0x52, 0x0b, 0x61, 0x67, 0x67, 0x72, 0x65, 0x67, 0x61,
	0x74, 0x65, 0x49, 0x64, 0x12, 0x24, 0x0a, 0x0d, 0x61, 0x67, 0x67, 0x72, 0x65, 0x67, 0x61, 0x74,
	0x65, 0x4e, 0x61, 0x6d, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0d, 0x61, 0x67, 0x67,
	0x72, 0x65, 0x67, 0x61, 0x74, 0x65, 0x4e, 0x61, 0x6d, 0x65, 0x12, 0x18, 0x0a, 0x07, 0x65, 0x76,
	0x65, 0x6e, 0x74, 0x49, 0x64, 0x18, 0x03, 0x20, 0x01, 0x28, 0x03, 0x52, 0x07, 0x65, 0x76, 0x65,
	0x6e, 0x74, 0x49, 0x64, 0x12, 0x1c, 0x0a, 0x09, 0x65, 0x76, 0x65, 0x6e, 0x74, 0x4e, 0x61, 0x6d,
	0x65, 0x18, 0x04, 0x20, 0x01, 0x28, 0x09, 0x52, 0x09, 0x65, 0x76, 0x65, 0x6e, 0x74, 0x4e, 0x61,
	0x6d, 0x65, 0x42, 0x1e, 0x5a, 0x1c, 0x67, 0x69, 0x74, 0x68, 0x75, 0x62, 0x2e, 0x63, 0x6f, 0x6d,
	0x2f, 0x77, 0x75, 0x79, 0x61, 0x7a, 0x69, 0x2f, 0x67, 0x64, 0x64, 0x64, 0x2f, 0x70, 0x72, 0x6f,
	0x74, 0x6f, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_message_proto_rawDescOnce sync.Once
	file_message_proto_rawDescData = file_message_proto_rawDesc
)

func file_message_proto_rawDescGZIP() []byte {
	file_message_proto_rawDescOnce.Do(func() {
		file_message_proto_rawDescData = protoimpl.X.CompressGZIP(file_message_proto_rawDescData)
	})
	return file_message_proto_rawDescData
}

var file_message_proto_msgTypes = make([]protoimpl.MessageInfo, 2)
var file_message_proto_goTypes = []interface{}{
	(*DomainEventMessageProto)(nil), // 0: proto.DomainEventMessageProto
	(*AbstractEvent)(nil),           // 1: proto.AbstractEvent
}
var file_message_proto_depIdxs = []int32{
	0, // [0:0] is the sub-list for method output_type
	0, // [0:0] is the sub-list for method input_type
	0, // [0:0] is the sub-list for extension type_name
	0, // [0:0] is the sub-list for extension extendee
	0, // [0:0] is the sub-list for field type_name
}

func init() { file_message_proto_init() }
func file_message_proto_init() {
	if File_message_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_message_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*DomainEventMessageProto); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_message_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*AbstractEvent); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_message_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   2,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_message_proto_goTypes,
		DependencyIndexes: file_message_proto_depIdxs,
		MessageInfos:      file_message_proto_msgTypes,
	}.Build()
	File_message_proto = out.File
	file_message_proto_rawDesc = nil
	file_message_proto_goTypes = nil
	file_message_proto_depIdxs = nil
}
