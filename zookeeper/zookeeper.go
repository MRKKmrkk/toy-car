package zookeeper

import (
	"fmt"
	"strings"
	"time"
	"toy-car/config"

	"github.com/samuel/go-zookeeper/zk"
)

type RichZookeeperConnection struct {
	zk.Conn
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
