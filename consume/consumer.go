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

func (consume *ToyCarConsumer) Consume() (*api.ConsumeResponse, error) {

	req := &api.ConsumeRequest{
		Topic:       consume.topic,
		PartitionId: 0,
	}

	res, err := consume.client.Consume(consume.context, req)
	if err != nil {
		return nil, err
	}

	return res, nil

}
