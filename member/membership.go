package member

import (
	"fmt"
	"toy-car/config"
	"toy-car/util"

	"github.com/samuel/go-zookeeper/zk"
)

type MemberShip struct {
	zkConn *zk.Conn
	config *config.Config
}

func NewMemberShip(config *config.Config) (*MemberShip, error) {

	conn, err := util.GetZKConn(config)
	if err != nil {
		return nil, err
	}

	return &MemberShip{
		zkConn: conn,
		config: config,
	}, nil

}

func (ms *MemberShip) Close() {

	ms.zkConn.Close()

}

func (ms *MemberShip) Init() error {

	_, err := ms.zkConn.Create(
		"/toy-car",
		[]byte(""),
		0,
		zk.WorldACL(zk.PermAll),
	)
	if err != nil && err.Error() != "zk: node already exists" {
		return err
	}

	_, err = ms.zkConn.Create(
		"/toy-car/brokers",
		[]byte(""),
		0,
		zk.WorldACL(zk.PermAll),
	)
	if err != nil && err.Error() != "zk: node already exists" {
		return err
	}

	return nil

}

// regist ip and port to zookeeper: /brokers/ids/[0...N]
func (ms *MemberShip) regist() error {

	s, err := ms.zkConn.Create(
		"/toy-car/brokers/ids",
		[]byte(ms.config.Server.ListenerAddress+":"+ms.config.Server.ListenerPort),
		3,
		zk.WorldACL(zk.PermAll),
	)
	fmt.Println(s)

	if err != nil {
		if err.Error() == "zk: node does not exist" {
			ms.Init()
		}
		return err
	}

	return nil

}
