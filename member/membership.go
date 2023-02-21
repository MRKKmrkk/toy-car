package member

import (
	"path"
	"strings"
	"toy-car/config"
	"toy-car/zookeeper"

	"github.com/samuel/go-zookeeper/zk"
)

type MemberShip struct {
	zkConn *zookeeper.RichZookeeperConnection
	config *config.Config
}

func NewMemberShip(config *config.Config) (*MemberShip, error) {

	conn, err := zookeeper.NewRichZookeeperConnection(config)
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

	isExists, err := ms.zkConn.IsExists("/toy-car/brokers")
	if err != nil {
		return err
	}

	if !isExists {
		ms.zkConn.RecurseCreate(
			"/toy-car/brokers",
			0,
			zk.WorldACL(zk.PermAll),
		)
	}

	isExists, err = ms.zkConn.IsExists("/toy-car/brokers/ids")
	if err != nil {
		return err
	}

	if !isExists {
		ms.zkConn.RecurseCreate(
			"/toy-car/brokers/ids",
			0,
			zk.WorldACL(zk.PermAll),
		)
	}

	return nil

}

// regist ip and port to zookeeper: /brokers/ids/[0...N]
// maybe need a lock here
func (ms *MemberShip) Join() error {

	_, err := ms.zkConn.Create(
		path.Join("/toy-car/brokers/ids/0"),
		[]byte(ms.config.Server.ListenerAddress+":"+ms.config.Server.ListenerPort),
		3,
		zk.WorldACL(zk.PermAll),
	)

	if err != nil {
		return err
	}

	return nil

}

func (ms *MemberShip) Leave() {

	ms.Close()

}

type Member struct {
	Address string
	Port    string
}

func NewMember(address string, port string) *Member {

	return &Member{
		Address: address,
		Port:    port,
	}

}

func (ms *MemberShip) GetMembers() ([]*Member, error) {

	paths, _, err := ms.zkConn.Children("/toy-car/brokers/ids")
	if err != nil {
		return nil, err
	}

	members := make([]*Member, len(paths))
	for i, p := range paths {
		data, _, err := ms.zkConn.Get(path.Join("/toy-car/brokers/ids", p))
		if err != nil {
			return nil, err
		}

		fields := strings.Split(string(data), ":")
		members[i] = NewMember(fields[0], fields[1])

	}

	return members, nil

}
