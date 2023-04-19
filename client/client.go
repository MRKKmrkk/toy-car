package client

import (
	"context"
	api "toy-car/api/v1"

	"google.golang.org/grpc"
)

type ToyCarClient struct {
	grpcClientConn *grpc.ClientConn
	Client         api.LogClient
	Context        context.Context
}

func NewToyCarClient(toyCarUri string) (*ToyCarClient, error) {

	clientOptions := []grpc.DialOption{grpc.WithInsecure()}
	cc, err := grpc.Dial(toyCarUri, clientOptions...)
	if err != nil {
		return nil, err
	}

	return &ToyCarClient{
		grpcClientConn: cc,
		Client:         api.NewLogClient(cc),
		Context:        context.Background(),
	}, nil

}

func (client *ToyCarClient) Close() error {

	return client.grpcClientConn.Close()

}
