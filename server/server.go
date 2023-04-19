package server

import (
	"context"
	"fmt"
	"io/ioutil"
	"net"
	api "toy-car/api/v1"
	"toy-car/config"
	"toy-car/util"
	"toy-car/wal"

	"github.com/samuel/go-zookeeper/zk"
	"google.golang.org/grpc"
)

type grpcServer struct {
	api.UnimplementedLogServer
	*config.Config
	Topics map[string]*wal.Topic
	zkConn *zk.Conn
}

func NewGRPCServer(config *config.Config) (*grpc.Server, *grpcServer, error) {

	gserver := grpc.NewServer()
	server, err := newgrpcServer(config)
	if err != nil {
		return nil, nil, err
	}

	api.RegisterLogServer(gserver, server)
	return gserver, server, nil

}

func newgrpcServer(config *config.Config) (*grpcServer, error) {

	topics := make(map[string]*wal.Topic)
	files, err := ioutil.ReadDir(config.LogDir)
	for _, f := range files {
		if !f.IsDir() {
			continue
		}

		t, err := wal.NewTopic(f.Name(), config)
		if err != nil {
			return nil, err
		}
		topics[f.Name()] = t
	}

	if err != nil {
		return nil, err
	}

	zkConn, err := util.GetZKConn(config)
	if err != nil {
		return nil, err
	}

	server := &grpcServer{
		Config: config,
		Topics: topics,
		zkConn: zkConn,
	}

	return server, nil

}

func (s *grpcServer) Produce(ctx context.Context, req *api.ProduceRequest) (*api.ProduceResponse, error) {

	topic, isExists := s.Topics[req.GetTopic()]
	if !isExists {
		t, err := wal.CreateTopic(req.GetTopic(), req.PartitionId+1, 1, s.Config)
		if err != nil {
			return nil, err
		}

		s.Topics[req.GetTopic()] = t
		topic = t
	}

	off, err := topic.Append(req.GetMsg(), req.GetPartitionId())
	if err != nil {
		return nil, err
	}

	return &api.ProduceResponse{Offset: off}, nil

}

func (s *grpcServer) Consume(ctx context.Context, req *api.ConsumeRequest) (*api.ConsumeResponse, error) {

	topic, ok := s.Topics[req.GetTopic()]
	if !ok {
		return nil, fmt.Errorf("cant found topic: %s", req.GetTopic())
	}

	msg, err := topic.Read(req.GetPartitionId(), req.Offset)
	if err != nil {
		return nil, err
	}

	return &api.ConsumeResponse{Msg: msg}, nil

}

func (s *grpcServer) CreateTopic(ctx context.Context, req *api.CreateTopicRequest) (*api.CreateTopicResponse, error) {

	_, err := wal.CreateTopic(
		req.GetTopic(),
		req.GetPartitions(),
		int(req.GetReplications()),
		s.Config,
	)

	res := &api.CreateTopicResponse{}
	if err != nil {
		res.ErrorMsg = err.Error()
		return nil, err
	}

	return res, nil

}

type ToyCarServer struct {
	server   *grpc.Server
	gserver  *grpcServer
	listener net.Listener
}

func NewToyCarServer(config *config.Config) (*ToyCarServer, error) {

	server, gserver, err := NewGRPCServer(config)
	if err != nil {
		return nil, err
	}

	l, err := net.Listen("tcp", config.Server.ListenerAddress+":"+config.Server.ListenerPort)
	if err != nil {
		return nil, err
	}

	return &ToyCarServer{
		server:   server,
		gserver:  gserver,
		listener: l,
	}, nil

}

func (tcs *ToyCarServer) Run() {

	tcs.server.Serve(tcs.listener)

}

func (tcs *ToyCarServer) Close() error {

	err := tcs.listener.Close()
	if err != nil {
		return err
	}
	tcs.server.Stop()
	tcs.gserver.zkConn.Close()

	for k := range tcs.gserver.Topics {
		err = tcs.gserver.Topics[k].Close()
		if err != nil {
			return err
		}
	}

	return nil

}
