package server

import (
	api "toy-car/api/v1"
	"toy-car/config"
	"toy-car/zookeeper"

	"github.com/golang/protobuf/proto"
	"github.com/samuel/go-zookeeper/zk"
)

const CONTROLLER_META_DATA_PATH = "/toy-car/controller"

type ToyCarController struct {
	zkConn *zookeeper.RichZookeeperConnection
	conf   *config.Config
}

func NewToyCarController(zkConn *zookeeper.RichZookeeperConnection, conf *config.Config) *ToyCarController {

	return &ToyCarController{
		zkConn: zkConn,
		conf:   conf,
	}

}

func (controller *ToyCarController) StartUp() error {

	// started to elect
	isExists, err := controller.zkConn.IsExists(CONTROLLER_META_DATA_PATH)
	if err != nil {
		return err
	}

	if !isExists {
		brokerMetaData, err := proto.Marshal(&api.ControllerMetaData{
			BrokerId:  controller.conf.Server.BrokerId,
			Timestamp: 0,
		})
		if err != nil {
			return err
		}

		_, err = controller.zkConn.Create(
			CONTROLLER_META_DATA_PATH,
			brokerMetaData,
			zk.FlagEphemeral,
			zk.WorldACL(zk.PermAll),
		)

		return nil
	}

	// started to listen to controller
	return nil

}

func (controller *ToyCarController) Elect() {
}

func (controller *ToyCarController) Close() {}
