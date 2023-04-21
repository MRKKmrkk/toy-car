package produce

import (
	"context"
	api "toy-car/api/v1"
	"toy-car/serialize"

	"google.golang.org/grpc"
)

const ACKS_NO_WAIT = 0
const ACKS_WAIT_LEADER = 1
const ACKS_WAIT_ALL = 2

type Partitioner func(topic string, host string, acks int, key string, value string) uint64

// todo: not compelete yet
func PARTITIONER_ROUND_ROBBIN(topic string, host string, acks int, key string, value string) uint64 {
	return 0
}

type ToyCarProducer struct {
	topic          string
	host           string
	acks           int
	grpcClientConn *grpc.ClientConn
	client         api.LogClient
	partitioner    Partitioner
	context        context.Context
	serializer     serialize.ToyCarSerializer
}

func NewToyCarProducer(topic string, host string) (*ToyCarProducer, error) {

	clientOptions := []grpc.DialOption{grpc.WithInsecure()}
	cc, err := grpc.Dial(host, clientOptions...)
	if err != nil {
		return nil, err
	}

	return &ToyCarProducer{
		topic:          topic,
		host:           host,
		acks:           ACKS_WAIT_ALL,
		grpcClientConn: cc,
		client:         api.NewLogClient(cc),
		context:        context.Background(),
		// the deafault partitioner is round robbin
		partitioner: PARTITIONER_ROUND_ROBBIN,
		serializer:  &serialize.SimpleStringSerializer{},
	}, nil

}

func (producer *ToyCarProducer) Send(key string, value string) (*api.ProduceResponse, error) {

	keyBytes, err := producer.serializer.Serialize(key)
	if err != nil {
		return nil, err
	}

	valueBytes, err := producer.serializer.Serialize(value)
	if err != nil {
		return nil, err
	}

	msg := &api.ProduceRequest{
		Msg: &api.Message{
			Key:   keyBytes,
			Value: valueBytes,
		},
		Topic: producer.topic,
		PartitionId: producer.partitioner(
			producer.topic,
			producer.host,
			producer.acks,
			key,
			value,
		),
		Ack: uint32(producer.acks),
	}

	return producer.client.Produce(producer.context, msg)

}
