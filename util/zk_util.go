package util

import (
	"time"
	"toy-car/config"

	"github.com/samuel/go-zookeeper/zk"
)

func GetZKConn(config *config.Config) (*zk.Conn, error) {

	conn, _, err := zk.Connect(
		config.Server.ZookeeperConnects,
		time.Second*5,
	)

	if err != nil {
		return nil, err
	}

	return conn, nil

}

func ZKPathExists(path string, conn *zk.Conn) (bool, error) {

	flag, _, err := conn.Exists(path)
	if err != nil {
		return false, err
	}

	return flag, nil

}
