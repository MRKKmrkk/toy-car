package client

import (
	"context"
	api "toy-car/api/v1"

	"google.golang.org/grpc"
)

type ToyCarClient struct {
	grpcClientConn *grpc.ClientConn
	client         api.LogClient
	context        context.Context
}

func NewToyCarClient(toyCarUri string) (*ToyCarClient, error) {

	clientOptions := []grpc.DialOption{grpc.WithInsecure()}
	cc, err := grpc.Dial(toyCarUri, clientOptions...)
	if err != nil {
		return nil, err
	}

	return &ToyCarClient{
		grpcClientConn: cc,
		client:         api.NewLogClient(cc),
		context:        context.Background(),
	}, nil

}

func (client *ToyCarClient) Close() error {

	return client.grpcClientConn.Close()

}
