package consume

import (
	"strings"
	"toy-car/serialize"
	"toy-car/util"
)

type OffsetRestFlag string

const (
	OFFSET_REST_EARLIEST OffsetRestFlag = "earliest"
	OFFSET_REST_LATEST   OffsetRestFlag = "latest"
)

type toyCarConsumeConf struct {
	Host               string
	Topic              string
	Serializer         serialize.ToyCarSerializer
	Deserializer       serialize.ToyCarDeserializer
	IsAutoCommit       bool
	AutoCommitRestFlag OffsetRestFlag
	GroupId            string
	ConsumeId          string
	KeyDeserializer    serialize.ToyCarDeserializer
	ValueDeserializer  serialize.ToyCarDeserializer
}

type ToyCarConsumeConfBuilder struct {
	host               string
	topic              string
	serializer         serialize.ToyCarSerializer
	deserializer       serialize.ToyCarDeserializer
	isAutoCommit       bool
	autoCommitRestFlag OffsetRestFlag
	groupId            string
	consumeId          string
	KeyDeserializer    serialize.ToyCarDeserializer
	ValueDeserializer  serialize.ToyCarDeserializer
}

func NewToyCarConsumerConfBuilder(host string, topic string, groupId string) *ToyCarConsumeConfBuilder {

	return &ToyCarConsumeConfBuilder{
		host:         host,
		topic:        topic,
		groupId:      groupId,
		isAutoCommit: true,
	}

}

func (builder *ToyCarConsumeConfBuilder) SetTopic(topic string) *ToyCarConsumeConfBuilder {

	builder.topic = topic
	return builder

}

func (builder *ToyCarConsumeConfBuilder) GetTopic() string {

	return builder.topic

}

func (builder *ToyCarConsumeConfBuilder) SetHost(host string) *ToyCarConsumeConfBuilder {

	builder.host = host
	return builder

}

func (builder *ToyCarConsumeConfBuilder) GetHost() string {

	return builder.host

}

func (builder *ToyCarConsumeConfBuilder) SetGroupId(groupId string) *ToyCarConsumeConfBuilder {

	builder.groupId = groupId
	return builder

}

func (builder *ToyCarConsumeConfBuilder) GetGroupId() string {

	return builder.groupId

}

func (builder *ToyCarConsumeConfBuilder) SetSerializer(ser serialize.ToyCarSerializer) *ToyCarConsumeConfBuilder {

	builder.serializer = ser
	return builder

}

func (builder *ToyCarConsumeConfBuilder) SetDeserializer(dser serialize.ToyCarDeserializer) *ToyCarConsumeConfBuilder {

	builder.deserializer = dser
	return builder

}

func (builder *ToyCarConsumeConfBuilder) DisableAutocommit() *ToyCarConsumeConfBuilder {

	builder.isAutoCommit = false
	return builder

}

func (builder *ToyCarConsumeConfBuilder) EnableAutocommit() *ToyCarConsumeConfBuilder {

	builder.isAutoCommit = true
	return builder

}

func (builder *ToyCarConsumeConfBuilder) SetAutoCommitResetFlag(flag OffsetRestFlag) *ToyCarConsumeConfBuilder {

	builder.autoCommitRestFlag = flag
	return builder

}

func (builder *ToyCarConsumeConfBuilder) GetAutoCommitResetFlag() OffsetRestFlag {

	return builder.autoCommitRestFlag

}

func (builder *ToyCarConsumeConfBuilder) SetConsumeId(consumeId string) *ToyCarConsumeConfBuilder {

	builder.consumeId = consumeId
	return builder

}

func (builder *ToyCarConsumeConfBuilder) GetConsumeId() string {

	return builder.consumeId

}

func (builder *ToyCarConsumeConfBuilder) SetKeyDeserializer(deser serialize.ToyCarDeserializer) *ToyCarConsumeConfBuilder {

	builder.KeyDeserializer = deser
	return builder

}

func (builder *ToyCarConsumeConfBuilder) GetKeyDeserializer() serialize.ToyCarDeserializer {

	return builder.KeyDeserializer

}

func (builder *ToyCarConsumeConfBuilder) SetValueDeserializer(deser serialize.ToyCarDeserializer) *ToyCarConsumeConfBuilder {

	builder.ValueDeserializer = deser
	return builder

}

func (builder *ToyCarConsumeConfBuilder) GetValueDeserializer() serialize.ToyCarDeserializer {

	return builder.ValueDeserializer

}

func (builder *ToyCarConsumeConfBuilder) Build() *toyCarConsumeConf {

	if builder.serializer == nil {
		builder.serializer = &serialize.SimpleStringSerializer{}
	}

	if builder.deserializer == nil {
		builder.deserializer = &serialize.SimpleStringDeserializer{}
	}

	if len(strings.Trim(string(builder.autoCommitRestFlag), " ")) == 0 {
		builder.autoCommitRestFlag = OFFSET_REST_LATEST
	}

	if len(strings.Trim(string(builder.consumeId), " ")) == 0 {
		builder.consumeId = "consumer-" + util.RandomString(36)
	}

	return &toyCarConsumeConf{
		Host:               builder.host,
		Topic:              builder.topic,
		Serializer:         builder.serializer,
		Deserializer:       builder.deserializer,
		IsAutoCommit:       builder.isAutoCommit,
		AutoCommitRestFlag: builder.autoCommitRestFlag,
		GroupId:            builder.groupId,
	}

}
