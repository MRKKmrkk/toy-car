package util_zk

import (
	"strings"
	"time"
	"toy-car/config"

	"github.com/samuel/go-zookeeper/zk"
)

func GetConn(config *config.Config) (*zk.Conn, error) {

	conn, _, err := zk.Connect(
		config.Server.ZookeeperConnects,
		time.Second*5,
	)

	if err != nil {
		return nil, err
	}

	return conn, nil

}

func Exists(path string, conn *zk.Conn) (bool, error) {

	flag, _, err := conn.Exists(path)
	if err != nil {
		return false, err
	}

	return flag, nil

}

func CreateNodeIfNotExists(path string, data []byte, flags int32, acl []zk.ACL, conn *zk.Conn) error {

	ok, err := Exists(path, conn)
	if err != nil {
		return err
	}

	if !ok {
		_, err = conn.Create(
			path,
			data,
			flags,
			acl,
		)

		if err != nil {
			return err
		}
	}

	return nil

}

func Join(path1 string, path2 string) string {

	fields1 := strings.Split(path1, "/")
	fields2 := strings.Split(path2, "/")

	var path string = ""
	for _, field := range fields1 {
		path += "/" + field
	}
	for _, field := range fields2 {
		path += "/" + field
	}

	return path

}

func CreateNodeCurIfNotExists(path string, data []byte, flags int32, acl []zk.ACL, conn *zk.Conn) error {

	fields := strings.Split(path, "/")
	cur := "/"
	curData := []byte("")

	for k, field := range fields {
		cur = Join(cur, field)
		if k == len(fields)-1 {
			curData = data
		}

		err := CreateNodeIfNotExists(cur, curData, flags, acl, conn)
		if err != nil {
			return err
		}
	}

	return nil

}
