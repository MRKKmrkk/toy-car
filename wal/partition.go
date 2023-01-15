package wal

import (
	"fmt"
	"os"
	"path"
	api "toy-car/api/v1"
	"toy-car/config"

	"github.com/golang/protobuf/proto"
)

type partition struct {
	partitionId uint64
	topicName   string
	config      *config.Config
	log         *log
}

func (p *partition) ParitionName() string {
	return fmt.Sprintf("%s-%d", p.topicName, p.partitionId)
}

func NewPartition(topicName string, partitionId uint64, config *config.Config) (*partition, error) {

	p := &partition{
		topicName:   topicName,
		partitionId: partitionId,
		config:      config,
	}

	dir := path.Join(config.LogDir, p.ParitionName())
	err := os.MkdirAll(dir, os.ModePerm)
	if err != nil && !os.IsExist(err) {
		return nil, err
	}

	l, err := NewLog(dir, config)
	if err != nil {
		return nil, err
	}
	p.log = l

	return p, nil

}

func (p *partition) Close() error {

	return p.log.Close()

}

func (p *partition) Remove() error {

	return p.log.Remove()

}

func (p *partition) Append(msg *api.Message) (uint64, error) {

	content, err := proto.Marshal(msg)
	if err != nil {
		return 0, err
	}

	off, err := p.log.Append(content)
	if err != nil {
		return 0, nil
	}

	return off, err

}

func (p *partition) Read(offset uint64) (*api.Message, error) {

	content, err := p.log.Read(offset)
	if err != nil {
		return nil, err
	}

	msg := &api.Message{}
	err = proto.Unmarshal(content, msg)
	if err != nil {
		return nil, err
	}

	return msg, nil

}
