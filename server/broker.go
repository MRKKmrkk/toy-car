package server

import (
	"encoding/binary"
	"fmt"
	logger "log"
	"os"
	"path"
	"strconv"
	"sync"
	api "toy-car/api/v1"
	"toy-car/config"
	"toy-car/zookeeper"

	"github.com/golang/protobuf/proto"
	"github.com/samuel/go-zookeeper/zk"
)

var (
	// the coder of the binary
	enc = binary.BigEndian
)

type LogEndOffset struct {
	file *os.File
	mu   sync.Mutex
}

type LeoManager struct {
	leos map[string]*LogEndOffset
	dir  string
}

func NewLeoManager(conf *config.Config) *LeoManager {

	manager := &LeoManager{}
	manager.dir = conf.LogDir
	manager.leos = make(map[string]*LogEndOffset)

	return manager

}

func (m *LeoManager) updateLeo(topic string, pid int, leo uint64) error {

	key := fmt.Sprintf("%s-%d", topic, pid)
	destination := path.Join(m.dir, key, "leo")

	leoObj, isExists := m.leos[key]
	if !isExists {
		leoFile, err := os.OpenFile(
			destination,
			os.O_RDWR|os.O_CREATE|os.O_APPEND,
			0644,
		)
		if err != nil {
			return err
		}

		leoObj = &LogEndOffset{
			file: leoFile,
		}

		m.leos[key] = leoObj

	}

	leoObj.mu.Lock()
	defer leoObj.mu.Unlock()

	return binary.Write(leoObj.file, enc, leo)

}

//func (m *LeoManager) getLeo(topic string, pid int) (uint64, error) {
//
//	leoObj, isExists := m.leos[fmt.Sprintf("%s-%d", topic, pid)]
//	if !isExists {
//		return 0, fmt.Errorf("get leo failed, cause %s-%d not found", topic, pid)
//	}
//
//	bytes := make([]byte, 8)
//	err := binary.Read(leoObj.file, enc, bytes)
//	if err != nil {
//		return 0, err
//	}
//
//	return uint64(bytes), nil
//
//}

func (m *LeoManager) Close() {

	for _, v := range m.leos {
		v.file.Close()
	}

}

type Broker struct {
	zkConn       *zookeeper.RichZookeeperConnection
	config       *config.Config
	IsController bool
	mu           sync.Mutex
	LeoManager   *LeoManager
}

func NewBroker(config *config.Config) (*Broker, error) {

	conn, err := zookeeper.NewRichZookeeperConnection(config)
	if err != nil {
		return nil, err
	}

	return &Broker{
		zkConn:       conn,
		config:       config,
		IsController: false,
		LeoManager:   NewLeoManager(config),
	}, nil

}

func (ms *Broker) Close() {

	ms.LeoManager.Close()
	ms.zkConn.Close()

}

func (ms *Broker) Init() error {

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

	isExists, err = ms.zkConn.IsExists("/toy-car/brokers/topics")
	if err != nil {
		return err
	}

	if !isExists {
		ms.zkConn.RecurseCreate(
			"/toy-car/brokers/topics",
			0,
			zk.WorldACL(zk.PermAll),
		)
	}

	return nil

}

// regist ip and port to zookeeper: /brokers/ids/[0...N]
// maybe need a lock here
func (ms *Broker) Join() error {

	broker := &api.BrokerMetaData{
		ListenerAddress: ms.config.Server.ListenerAddress,
		ListenerPort:    ms.config.Server.ListenerPort,
	}
	brokerData, err := proto.Marshal(broker)
	if err != nil {
		return err
	}

	_, err = ms.zkConn.Create(
		path.Join("/toy-car/brokers/ids/"+strconv.Itoa(int(ms.config.Server.BrokerId))),
		brokerData,
		zk.FlagEphemeral,
		zk.WorldACL(zk.PermAll),
	)

	if err != nil {
		return err
	}

	return nil

}

func (ms *Broker) Leave() {

	ms.Close()

}

func (broker *Broker) Elect() error {

	for {

		isExists, err := broker.zkConn.IsExists(CONTROLLER_META_DATA_PATH)
		if err != nil {
			return err
		}
		if isExists {
			break
		}

		brokerMetaData, err := proto.Marshal(&api.ControllerMetaData{
			BrokerId:  broker.config.Server.BrokerId,
			Timestamp: 0,
		})
		if err != nil {
			return err
		}

		_, err = broker.zkConn.Create(
			CONTROLLER_META_DATA_PATH,
			brokerMetaData,
			zk.FlagEphemeral,
			zk.WorldACL(zk.PermAll),
		)
		if err != nil {
			if err.Error() == "zk: node already exists" {
				return nil
			}
			return err
		}

		broker.IsController = true
		logger.Printf("broker %d elect to controller", broker.config.Server.BrokerId)
	}

	return nil

}

func (broker *Broker) watchOnController() {

	_, _, ch, err := broker.zkConn.ExistsW(CONTROLLER_META_DATA_PATH)
	if err != nil {
		panic(err)
	}

	for {
		select {
		case event := <-ch:
			if event.Type == zk.EventNodeDeleted {
				// relect
				err := broker.Elect()
				if err != nil {
					// todo: some thing wrong here
					panic(err)
				}
			}
		}
	}

}

func (broker *Broker) StartUp() error {

	// join ids
	err := broker.Join()
	if err != nil {
		logger.Printf("some error happened when broker join the ids: %v", err)
		return err
	}

	// started to elect
	err = broker.Elect()
	if err != nil {
		logger.Printf("some error happened when broker elected: %v", err)
		return err
	}

	if broker.IsController {
		return nil
	}

	// started to listen to controller
	go broker.watchOnController()
	return nil

}

// only broker of controller run this method
// monitor: /toy-car/brokers/ids , change topic metadata and partition state when broker leaved
func (broker *Broker) maintainISR() error {

	// todo : maybe need return error here
	//if !broker.IsController {
	//	return nil
	//}

	_, _, ch, err := broker.zkConn.ChildrenW("/toy-car/brokers/ids")
	if err != nil {
		return err
	}

	for {
		select {
		case event := <-ch:
			if event.Type == zk.EventNodeChildrenChanged {
				fmt.Println(event.State.String())
				fmt.Println(event.Path)
				fmt.Printf("event: %v\n", event)
			}

		}
	}

}

//func (broker *Broker) updateLeo() error {
//
//	return nil
//
//}
