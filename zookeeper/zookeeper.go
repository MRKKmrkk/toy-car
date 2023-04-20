package zookeeper

import (
	"fmt"
	pt "path"
	"strconv"
	"strings"
	"time"
	api "toy-car/api/v1"
	"toy-car/config"

	"github.com/golang/protobuf/proto"
	"github.com/samuel/go-zookeeper/zk"
)

func GetTopicZookeeperPath() string {

	return "/toy-car/brokers/topics"

}

func GetParititionZookeeperPath(topic string) string {

	return fmt.Sprintf("/toy-car/brokers/topics/%s/partitions", topic)

}

func GetPartitionStateZookeeperPath(topic string, paritionId int) string {

	return fmt.Sprintf("/toy-car/brokers/topics/%s/partitions/%d/state", topic, paritionId)

}

func GetTopicMetadataZookeeperPath(topic string) string {

	return fmt.Sprintf("/toy-car/brokers/topics/%s", topic)

}

const FlagLasting = 0

type RichZookeeperConnection struct {
	zk.Conn
}

var ZKConn *RichZookeeperConnection

func GetOrCreateZookeeperConnection() (*RichZookeeperConnection, error) {

	if ZKConn == nil {
		conf, err := config.NewConfig()
		if err != nil {
			return nil, err
		}

		ZKConn, err = NewRichZookeeperConnection(conf)
		if err != nil {
			return nil, err
		}

	}

	return ZKConn, nil
}

// create rich zookeeper connection instance
func NewRichZookeeperConnection(config *config.Config) (*RichZookeeperConnection, error) {

	conn, _, err := zk.Connect(
		config.Server.ZookeeperConnects,
		time.Second*5,
	)

	if err != nil {
		return nil, err
	}

	return &RichZookeeperConnection{*conn}, nil

}

// return false and error if path not exists
func (conn *RichZookeeperConnection) IsExists(path string) (bool, error) {

	flag, _, err := conn.Exists(path)
	if err != nil {
		return false, err
	}

	return flag, nil

}

func (conn *RichZookeeperConnection) recurseCreate(path string, flag int32, acl []zk.ACL) error {

	i := len(path) - 1
	for path[i] != 47 {
		i--
	}

	var err error
	if i != 0 {
		err = conn.recurseCreate(path[:i], flag, acl)
	}

	if err != nil {
		return err
	}
	if isExists, err := conn.IsExists(path); err == nil && !isExists {
		_, err := conn.Create(
			path,
			[]byte(""),
			flag,
			acl,
		)

		return err
	}

	return err

}

// create node cursizly and have not data
func (conn *RichZookeeperConnection) RecurseCreate(path string, flag int32, acl []zk.ACL) error {

	if strings.HasSuffix(path, "/") {
		path = path[:len(path)-1]
	}

	if len(strings.Trim(path, " ")) == 0 {
		return fmt.Errorf("argument path cant be empty")
	}

	if !strings.HasPrefix(path, "/") {
		return fmt.Errorf("argument path must start with '/' ")
	}

	return conn.recurseCreate(path, flag, acl)

}

func (conn *RichZookeeperConnection) RecurseDelete(path string) error {

	nodes, _, err := conn.Children(path)
	if err != nil {
		return err
	}

	if len(nodes) != 0 {
		for _, node := range nodes {
			err = conn.RecurseDelete(pt.Join(path, node))
			if err != nil {
				return err
			}
		}
	}

	_, stat, err := conn.Get(path)
	if err != nil {
		return err
	}

	return conn.Delete(path, stat.Version)

}

func (conn *RichZookeeperConnection) ListBrokerId() ([]int, error) {

	paths, _, _ := conn.Children("/toy-car/brokers/ids")
	ids := make([]int, len(paths))

	for i, v := range paths {
		num, err := strconv.Atoi(v)
		if err != nil {
			return nil, err
		}

		ids[i] = num
	}

	return ids, nil

}

func (conn *RichZookeeperConnection) ListTopics() ([]string, error) {

	topics, _, err := conn.Children("/toy-car/brokers/topics")
	if err != nil {
		return nil, err
	}

	return topics, nil

}

func (conn *RichZookeeperConnection) ListParitionIds(topic string) ([]int, error) {

	partitionIds, _, err := conn.Children(fmt.Sprintf("/toy-car/brokers/topics/%s/partitions", topic))
	if err != nil {
		return nil, err
	}

	arr := make([]int, len(partitionIds))
	for i, _ := range partitionIds {
		num, err := strconv.Atoi(partitionIds[i])
		if err != nil {
			return nil, err
		}

		arr[i] = num
	}

	return arr, nil

}

func (conn *RichZookeeperConnection) GetParititionStateVar(ps *api.PartitionState, topic string, partitionId int) error {

	bytes, _, err := conn.Get(GetPartitionStateZookeeperPath(topic, partitionId))
	if err != nil {
		return err
	}
	err = proto.Unmarshal(bytes, ps)

	if err != nil {
		return err
	}

	return nil

}

func (conn *RichZookeeperConnection) GetParititionState(topic string, partitionId int) (*api.PartitionState, error) {

	ps := &api.PartitionState{}
	err := conn.GetParititionStateVar(ps, topic, partitionId)
	if err != nil {
		return nil, err
	}

	return ps, nil

}
func (conn *RichZookeeperConnection) GetTopicMetadataVar(meta *api.TopicMetaData, topic string) error {

	bytes, _, err := conn.Get(GetTopicMetadataZookeeperPath(topic))
	if err != nil {
		return err
	}
	err = proto.Unmarshal(bytes, meta)

	if err != nil {
		return err
	}

	return nil

}

func (conn *RichZookeeperConnection) GetTopicMetadata(topic string) (*api.TopicMetaData, error) {

	meta := &api.TopicMetaData{}
	err := conn.GetTopicMetadataVar(meta, topic)
	if err != nil {
		return nil, err
	}

	return meta, nil

}

func (conn *RichZookeeperConnection) SetTopicMetadataVar(topic string, meta *api.TopicMetaData) error {

	metaPath := GetTopicMetadataZookeeperPath(topic)
	_, stat, err := conn.Get(metaPath)
	if err != nil {
		return err
	}

	bytes, err := proto.Marshal(meta)
	if err != nil {
		return err
	}

	_, err = conn.Set(
		metaPath,
		bytes,
		stat.Version,
	)

	return err

}

func (conn *RichZookeeperConnection) SetPartitionStateVar(topic string, partitionId int, state *api.PartitionState) error {

	statePath := GetPartitionStateZookeeperPath(topic, partitionId)
	_, stat, err := conn.Get(statePath)
	if err != nil {
		return err
	}

	bytes, err := proto.Marshal(state)
	if err != nil {
		return err
	}

	_, err = conn.Set(
		statePath,
		bytes,
		stat.Version,
	)

	return err

}

func (conn *RichZookeeperConnection) IsTopicExists(topic string) (bool, error) {

	topics, err := conn.ListTopics()
	if err != nil {
		return false, err
	}

	for _, t := range topics {
		if topic == t {
			return true, nil
		}
	}

	return false, nil

}
