package produce

import (
	"context"
	api "toy-car/api/v1"

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
	}, nil

}

func (producer *ToyCarProducer) Send(key string, value string) {

	msg := &api.ProduceRequest{
		Msg: &api.Message{
			Key:   key,
			Value: value,
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

	producer.client.Produce(producer.context, msg)

}
