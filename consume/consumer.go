package consume

import (
	"context"
	api "toy-car/api/v1"
	"toy-car/serialize"

	"google.golang.org/grpc"
)

type ToyCarConsumer struct {
	topic              string
	host               string
	grpcClientConn     *grpc.ClientConn
	client             api.LogClient
	context            context.Context
	serializer         serialize.ToyCarSerializer
	deserializer       serialize.ToyCarDeserializer
	keyDeserializer    serialize.ToyCarDeserializer
	valueDeserializer  serialize.ToyCarDeserializer
	isAutoCommit       bool
	autoCommitRestFlag OffsetRestFlag
	groupId            string
	consumeId          string
	consumeOffset      uint64
}

func NewToyCarConsumer(config *toyCarConsumeConf) (*ToyCarConsumer, error) {

	clientOptions := []grpc.DialOption{grpc.WithInsecure()}
	cc, err := grpc.Dial(config.Host, clientOptions...)
	if err != nil {
		return nil, err
	}

	return &ToyCarConsumer{
		topic:              config.Topic,
		host:               config.Host,
		grpcClientConn:     cc,
		context:            context.Background(),
		serializer:         config.Serializer,
		deserializer:       config.Deserializer,
		isAutoCommit:       config.IsAutoCommit,
		autoCommitRestFlag: config.AutoCommitRestFlag,
	}, nil

}

type Message struct {
	Key    interface{}
	Value  interface{}
	Offset uint64
}

func (consume *ToyCarConsumer) ConsumeByOffset(off uint64) (*Message, error) {

	req := &api.ConsumeRequest{
		Topic:       consume.topic,
		PartitionId: 0,
		Offset:      off,
	}

	res, err := consume.client.Consume(consume.context, req)
	if err != nil {
		return nil, err
	}

	msg := &Message{}
	key, err := consume.keyDeserializer.Deserialize(res.Msg.GetKey())
	if err != nil {
		return nil, err
	}
	value, err := consume.valueDeserializer.Deserialize(res.Msg.GetValue())
	if err != nil {
		return nil, err
	}

	msg.Key = key
	msg.Value = value
	msg.Offset = res.Msg.GetOffset()

	return msg, nil

}
