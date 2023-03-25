package zookeeper

import (
	"fmt"
	pt "path"
	"strconv"
	"strings"
	"time"
	"toy-car/config"

	"github.com/samuel/go-zookeeper/zk"
)

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
