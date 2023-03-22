package wal

import (
	"fmt"
	"io/fs"
	"io/ioutil"
	"math/rand"
	"strconv"
	"strings"
	api "toy-car/api/v1"
	"toy-car/config"
	"toy-car/util"
	"toy-car/zookeeper"

	"github.com/golang/protobuf/proto"
	"github.com/samuel/go-zookeeper/zk"
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

func allocateReplicaByPolicy(conn *zookeeper.RichZookeeperConnection, replicaNum int) ([]int, error) {

	c, err := config.NewConfig()
	if err != nil {
		return nil, err
	}

	ids, err := conn.ListBrokerId()
	if err != nil {
		return nil, err
	}

	switch c.Replica.AllocationPolicy {
	case "random":
		rand.Shuffle(len(ids), func(i, j int) { ids[i], ids[j] = ids[j], ids[i] })
	}

	return ids[:replicaNum], nil

}

func CreateTopic(topicName string, partitionNum uint64, replicaNum int, config *config.Config) (*Topic, error) {

	_, err := listLogDir(config)
	if err != nil {
		return nil, err
	}

	conn, err := zookeeper.GetOrCreateZookeeperConnection()
	if err != nil {
		return nil, err
	}

	topicMetaData := &api.TopicMetaData{
		Version: 1,
	}
	topicMetaData.Partitions = make(map[int32]*api.BrokerIds)

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

		// allocate repplicates to topic
		ids, err := allocateReplicaByPolicy(conn, replicaNum)
		if err != nil {
			return nil, err
		}

		topicMetaData.Partitions[int32(i)] = &api.BrokerIds{}
		for _, v := range ids {
			topicMetaData.Partitions[int32(i)].BrokerId = append(topicMetaData.Partitions[int32(i)].BrokerId, int32(v))
		}

		// log parititon state in zookeeper
		state := &api.PartitionState{
			ControllerEpoch: 0,
			Version:         1,
			ISR:             util.ArrayIntToInt32(ids),
		}
		bytes, err := proto.Marshal(state)
		if err != nil {
			return nil, err
		}
		_, err = conn.Create(
			fmt.Sprintf("/toy-car/brokers/topics/%s/partitions/%d/state", topicName, i),
			bytes,
			zookeeper.FlagLasting,
			zk.WorldACL(zk.PermAll),
		)
		if err != nil {
			return nil, err
		}

	}

	// log topic information in zookeeper
	bytes, err := proto.Marshal(topicMetaData)
	if err != nil {
		return nil, err
	}
	_, err = conn.Create(
		fmt.Sprintf("/toy-car/brokers/topics/%s", topicName),
		bytes,
		zookeeper.FlagLasting,
		zk.WorldACL(zk.PermAll),
	)
	if err != nil {
		return nil, err
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
