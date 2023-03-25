package server

import (
	logger "log"
	"path"
	"strconv"
	api "toy-car/api/v1"
	"toy-car/config"
	"toy-car/zookeeper"

	"github.com/golang/protobuf/proto"
	"github.com/samuel/go-zookeeper/zk"
)

type Broker struct {
	zkConn       *zookeeper.RichZookeeperConnection
	config       *config.Config
	IsController bool
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
	}, nil

}

func (ms *Broker) Close() {

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
