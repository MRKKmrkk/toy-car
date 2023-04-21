package a

import (
	"context"

	// keyword 'api' seems can make go file read mod into child directory
	api "distribute-exp/wal/internal/api/v1"

	"google.golang.org/grpc"
)

type CommitLog interface {
	Append(*api.Record) (uint64, error)
	Read(uint64) (*api.Record, error)
}

type Config struct {
	CommitLog CommitLog
}

type grpcServer struct {
	api.UnimplementedLogServer
	*Config
}

func NewGRPCServer(config *Config) (*grpc.Server, error) {

	gserver := grpc.NewServer()
	server, err := newgrpcServer(config)
	if err != nil {
		return nil, err
	}

	api.RegisterLogServer(gserver, server)
	return gserver, nil

}

func newgrpcServer(config *Config) (*grpcServer, error) {

	server := &grpcServer{
		Config: config,
	}

	return server, nil

}

func (s *grpcServer) Produce(ctx context.Context, req *api.ProduceRequest) (*api.ProduceResponse, error) {

	off, err := s.CommitLog.Append(req.Record)
	if err != nil {
		return nil, err
	}

	return &api.ProduceResponse{Offset: off}, nil

}

func (s *grpcServer) Consume(ctx context.Context, req *api.ConsumeRequest) (*api.ConsumeResponse, error) {

	record, err := s.CommitLog.Read(req.Offset)
	if err != nil {
		return nil, err
	}

	return &api.ConsumeResponse{Record: record}, nil

}

func (s *grpcServer) ProduceStream(stream api.Log_ProduceStreamServer) error {
	for {
		// receive produce request from client
		req, err := stream.Recv()
		if err != nil {
			return err
		}

		// staring produce log with produce request
		res, err := s.Produce(stream.Context(), req)
		if err != nil {
			return err
		}

		// send produce response back to the client
		err = stream.Send(res)
		if err != nil {
			return err
		}
	}
}

func (s *grpcServer) ConsumeStream(req *api.ConsumeRequest, stream api.Log_ConsumeStreamServer) error {

	for {
		select {
		// stop this method when server close gracefully
		case <-stream.Context().Done():
			return nil
		default:
			// consume record with the consume request
			res, err := s.Consume(stream.Context(), req)

			// handle errors
			switch err.(type) {
			case nil:
			case api.ErrOffsetOutOfRange:
				continue
			default:
				return err
			}

			// send consume response back
			err = stream.Send(res)
			if err != nil {
				return err
			}
			// growing offset
			req.Offset++
		}
	}

}
