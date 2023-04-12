package server

import (
	"fmt"
	"toy-car/zookeeper"

	logger "log"

	"github.com/samuel/go-zookeeper/zk"
)

type BrokerMonitorManager struct {
	monitorMap map[int]*BrokerMonitor
	conn       *zookeeper.RichZookeeperConnection
	isRun      bool
}

func NewBrokerMonitorManager() *BrokerMonitorManager {

	return &BrokerMonitorManager{
		monitorMap: make(map[int]*BrokerMonitor),
		isRun:      false,
	}

}

func (manager *BrokerMonitorManager) unWatch(id int) {

	_, ok := manager.monitorMap[id]
	if ok {
		delete(manager.monitorMap, id)
	}

}

func (manager *BrokerMonitorManager) watch(id int) {

	_, ok := manager.monitorMap[id]
	if !ok {
		manager.monitorMap[id] = NewBrokerMonitor(id, manager)
		go manager.monitorMap[id].watch()
	}

}

func (manager *BrokerMonitorManager) Close() {

	manager.isRun = false

	for k, _ := range manager.monitorMap {
		manager.monitorMap[k].Close()
	}

}

func (manager *BrokerMonitorManager) Watch() {

	manager.isRun = true
	_, _, ch, _ := manager.conn.ChildrenW("/toy-car/brokers/ids")

	for {

		if !manager.isRun {
			break
		}

		select {
		case event := <-ch:
			if event.Type == zk.EventNodeChildrenChanged {
				ids, err := manager.conn.ListBrokerId()
				if err != nil {
					break
				}

				for _, v := range ids {
					manager.watch(v)
				}
			}
		}
	}

}

type BrokerMonitor struct {
	manager       *BrokerMonitorManager
	watchBrokerId int
	conn          *zookeeper.RichZookeeperConnection
	isRun         bool
}

func NewBrokerMonitor(watchBrokerId int, manager *BrokerMonitorManager) *BrokerMonitor {

	return &BrokerMonitor{
		watchBrokerId: watchBrokerId,
		manager:       manager,
		conn:          manager.conn,
		isRun:         false,
	}

}

func (monitor *BrokerMonitor) Close() {

	monitor.isRun = false
	monitor.manager.unWatch(monitor.watchBrokerId)

}

func (monitor *BrokerMonitor) watch() {

	logger.Printf("monitor watch on %d\n", monitor.watchBrokerId)

	_, _, ch, _ := monitor.conn.ExistsW(fmt.Sprintf("/toy-car/brokers/ids/%d", monitor.watchBrokerId))
	for {

		if !monitor.isRun {
			break
		}

		select {
		case event := <-ch:
			if event.Type == zk.EventNodeDeleted {
				// change metadata of topic metadata
				monitor.Close()
				break
			}
			if event.Type == zk.EventNotWatching {
				monitor.Close()
				break
			}
		}
	}

	logger.Printf("monitor which watch on %d gonna be close", monitor.watchBrokerId)

}
