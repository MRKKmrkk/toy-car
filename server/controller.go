package server

import (
	api "toy-car/api/v1"
	"toy-car/util"
	"toy-car/zookeeper"

	logger "log"

	"github.com/samuel/go-zookeeper/zk"
)

const CONTROLLER_META_DATA_PATH = "/toy-car/controller"

type MetaDataIndexMap struct {
	cache  map[int]map[string][]int
	zkConn *zookeeper.RichZookeeperConnection
}

func NewMetaDataIndexMap(zkConn *zookeeper.RichZookeeperConnection) *MetaDataIndexMap {

	return &MetaDataIndexMap{
		cache:  make(map[int]map[string][]int),
		zkConn: zkConn,
	}

}

func (m *MetaDataIndexMap) Sync() error {

	topics, err := m.zkConn.ListTopics()
	if err != nil {
		return err
	}
	if len(topics) == 0 {
		return nil
	}

	meta := &api.TopicMetaData{}
	for _, topic := range topics {
		err := m.zkConn.GetTopicMetadataVar(meta, topic)
		if err != nil {
			return err
		}

		// update metadata
		for pid, brokerIds := range meta.Partitions {
			for _, v := range brokerIds.BrokerId {
				_, exists := m.cache[int(v)]
				if !exists {
					m.cache[int(v)] = make(map[string][]int)
				}

				_, exists = m.cache[int(v)][topic]
				if !exists {
					m.cache[int(v)][topic] = make([]int, 0)
				}

				m.cache[int(v)][topic] = append(m.cache[int(v)][topic], int(pid))

			}
		}
	}

	return nil

}

type ToyCarController struct {
	ids           []int
	zkConn        *zookeeper.RichZookeeperConnection
	isRun         bool
	metaDataIndex *MetaDataIndexMap
}

func NewToyCarController(zkConn *zookeeper.RichZookeeperConnection) *ToyCarController {

	return &ToyCarController{
		zkConn:        zkConn,
		isRun:         false,
		metaDataIndex: NewMetaDataIndexMap(zkConn),
	}

}

func (controller *ToyCarController) init() error {

	var err error

	// cache broker ids
	controller.ids, err = controller.zkConn.ListBrokerId()
	if err != nil {
		return err
	}

	// cache metadata
	return controller.metaDataIndex.Sync()

}

func (controller *ToyCarController) Close() {

	controller.isRun = false

}

func contains(arr []int, num int) bool {
	for _, v := range arr {
		if v == num {
			return true
		}
	}
	return false
}

//func leavedNodes(old []int, now []int) []int {
//
//	lns := make([]int, 0)
//
//	for _, id := range old {
//		if !contains(now, id) {
//			lns = append(lns, id)
//		}
//	}
//
//	return lns
//
//}

func (controller *ToyCarController) ListDeadNodeIds() ([]int, error) {

	lns := make([]int, 0)

	now, err := controller.zkConn.ListBrokerId()
	if err != nil {
		return nil, err
	}

	for _, id := range controller.ids {
		if !contains(now, id) {
			lns = append(lns, id)
		}
	}

	return lns, nil

}

func (controller *ToyCarController) deleteIdFromParitionState(id int) error {

	logger.Printf("try to remove broker: %d from zookeeper metadata", id)

	ps := &api.PartitionState{}

	for topic, paritions := range controller.metaDataIndex.cache[id] {
		for _, pid := range paritions {
			err := controller.zkConn.GetParititionStateVar(ps, topic, pid)
			if err != nil {
				return err
			}

			ps.ISR = util.RemoveElementOnInt32Array(ps.ISR, int32(id))
			var leaderId int32
			if len(ps.ISR) == 0 {
				leaderId = -1
			} else {
				leaderId = ps.ISR[0]
			}

			if ps.GetLeader() == int32(id) {
				ps.Leader = leaderId
				logger.Printf("change {topic: %s parition: %d} leader to %d\n", topic, pid, leaderId)
			}

			err = controller.zkConn.SetPartitionStateVar(topic, pid, ps)
			if err != nil {
				return err
			}

		}
	}

	logger.Printf("remove broker: %d success", id)
	return nil

}

func (controller *ToyCarController) deleteIdFromTopicMetaData(id int) error {

	metadata := &api.TopicMetaData{}
	for topic, pids := range controller.metaDataIndex.cache[id] {

		err := controller.zkConn.GetTopicMetadataVar(metadata, topic)
		if err != nil {
			return err
		}

		for _, pid := range pids {

			metadata.Partitions[int32(pid)].BrokerId = util.RemoveElementOnInt32Array(
				metadata.Partitions[int32(pid)].BrokerId,
				int32(id),
			)

			err = controller.zkConn.SetTopicMetadataVar(topic, metadata)
			if err != nil {
				return err
			}

			logger.Printf("change topic metadata{topic: %s parition: %d}", topic, pid)
		}

	}

	return nil

}

func (controller *ToyCarController) idsMonitor() {

	_, _, ch, err := controller.zkConn.ChildrenW("/toy-car/brokers/ids")
	if err != nil {
		logger.Fatal(err)
	}

	for {
		if !controller.isRun {
			break
		}

		select {
		case event := <-ch:
			if event.Type == zk.EventNodeChildrenChanged {
				deadIds, err := controller.ListDeadNodeIds()
				if err != nil {
					logger.Fatal(err)
				}

				// update metadata
				for _, id := range deadIds {
					logger.Printf("broker: %d out offline", id)

					// update topic metadata
					err = controller.deleteIdFromTopicMetaData(id)
					if err != nil {
						logger.Fatal(err)
					}

					// update parition state
					err = controller.deleteIdFromParitionState(id)
					if err != nil {
						logger.Fatal(err)
					}
				}
			}
			if event.Type == zk.EventNotWatching {
				controller.Close()
			}
		}
	}

}

func (controller *ToyCarController) Run() {

	controller.isRun = true
	go controller.idsMonitor()

}
