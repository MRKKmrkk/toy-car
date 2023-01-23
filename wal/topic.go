package wal

import (
	"fmt"
	"io/fs"
	"io/ioutil"
	"strconv"
	"strings"
	api "toy-car/api/v1"
	"toy-car/config"
)

type Topic struct {
	topicName  string
	partitions map[uint64]*partition
	config     *config.Config
}

func (t *Topic) TopicName() string {
	return t.topicName
}

func listLogDir(config *config.Config) ([]fs.FileInfo, error) {

	files, err := ioutil.ReadDir(config.LogDir)
	if err != nil {
		return nil, err
	}

	return files, nil
}

func CreateTopic(topicName string, partitionNum uint64, config *config.Config) (*Topic, error) {

	_, err := listLogDir(config)
	if err != nil {
		return nil, err
	}

	topic := &Topic{
		topicName:  topicName,
		config:     config,
		partitions: make(map[uint64]*partition),
	}

	for i := uint64(0); i < partitionNum; i++ {
		p, err := NewPartition(topicName, i, config)
		if err != nil {
			return nil, err
		}
		topic.partitions[i] = p
	}

	return topic, nil

}

func NewTopic(topicName string, config *config.Config) (*Topic, error) {

	topic := &Topic{
		topicName:  topicName,
		config:     config,
		partitions: make(map[uint64]*partition),
	}

	files, err := listLogDir(config)
	if err != nil {
		return nil, err
	}

	if len(files) == 0 {
		return nil, fmt.Errorf("topic %s must have at least 1 partition, but found zero", topicName)
	}

	for _, file := range files {
		fields := strings.Split(file.Name(), "-")
		if len(fields) != 2 {
			return nil, fmt.Errorf("partition name:%s is not illegal", file.Name())
		}

		if fields[0] != topicName {
			continue
		}

		id, err := strconv.ParseInt(fields[1], 10, 64)
		if err != nil {
			return nil, err
		}

		p, err := NewPartition(topicName, uint64(id), config)
		if err != nil {
			return nil, err
		}

		topic.partitions[uint64(id)] = p

	}

	return topic, nil
}

func (t *Topic) Close() error {

	for k := range t.partitions {
		err := t.partitions[k].Close()
		if err != nil {
			return err
		}
	}

	return nil

}

func (t *Topic) Remove() error {

	for k := range t.partitions {
		err := t.partitions[k].Remove()
		if err != nil {
			return err
		}
	}

	return nil

}

func (t *Topic) Append(msg *api.Message, paritionId uint64) (uint64, error) {

	p, ok := t.partitions[paritionId]
	if !ok {
		return 0, fmt.Errorf("cant not found parition by id: %d on topic %s", paritionId, t.topicName)
	}

	return p.Append(msg)

}

func (t *Topic) Read(paritionId uint64, offset uint64) (*api.Message, error) {

	p, ok := t.partitions[paritionId]
	if !ok {
		return nil, fmt.Errorf("cant not found parition by id: %d on topic %s", paritionId, t.topicName)
	}

	return p.Read(offset)

}
