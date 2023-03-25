package main

import (
	"fmt"
	"os"
	"path"
	api "toy-car/api/v1"
	"toy-car/zookeeper"

	"github.com/golang/protobuf/proto"
)

// get all of metadata and display on console

func noError(err error) {

	if err != nil {
		panic(err)
	}

}

func getBrokerInformation(conn *zookeeper.RichZookeeperConnection, content *string) error {

	*content += "\n===== brokers =====\n"

	controllerMetaData := &api.ControllerMetaData{}
	data, _, err := conn.Get("/toy-car/controller")
	if err != nil {
		return err
	}
	proto.Unmarshal(data, controllerMetaData)
	*content += fmt.Sprintf("controller id: %d\n", controllerMetaData.BrokerId)

	ids, err := conn.ListBrokerId()
	if err != nil {
		return err
	}

	for _, v := range ids {
		*content += fmt.Sprintf("running broker id: %d\n", v)
	}

	return nil

}

func getTopicInformation(conn *zookeeper.RichZookeeperConnection, content *string) error {

	*content += "\n===== topics =====\n"

	topics, _, err := conn.Children("/toy-car/brokers/topics")
	if err != nil {
		return err
	}

	for _, topic := range topics {

		*content += fmt.Sprintf("%s:\n", topic)
		paritionIds, _, err := conn.Children(fmt.Sprintf("/toy-car/brokers/topics/%s/partitions", topic))
		if err != nil {
			return err
		}

		for _, paritionId := range paritionIds {

			*content += fmt.Sprintf("\tpartition id:%s\n", paritionId)

			data, _, err := conn.Get(fmt.Sprintf("/toy-car/brokers/topics/%s/partitions/%s/state", topic, paritionId))
			if err != nil {
				return err
			}

			partitionState := &api.PartitionState{}
			err = proto.Unmarshal(data, partitionState)
			if err != nil {
				return err
			}

			*content += fmt.Sprintf("\t\tleader: %d\n\t\tisr: %v\n", partitionState.GetLeader(), partitionState.GetISR())

		}

	}

	return nil
}

func show(conn *zookeeper.RichZookeeperConnection) {

	content := ""
	err := getBrokerInformation(conn, &content)
	noError(err)

	err = getTopicInformation(conn, &content)
	noError(err)

	fmt.Println(content)

}

func clean(conn *zookeeper.RichZookeeperConnection) {

	topics, _, err := conn.Children("/toy-car/brokers/topics")
	noError(err)
	if len(topics) == 0 {
		return
	}

	for _, topic := range topics {
		path := path.Join("/toy-car/brokers/topics", topic)

		err = conn.RecurseDelete(path)
		noError(err)
		fmt.Printf(fmt.Sprintf("remove zookeeper metadata: %s\n", path))
	}

}

func main() {

	conn, err := zookeeper.GetOrCreateZookeeperConnection()
	noError(err)
	defer conn.Close()

	if len(os.Args) != 2 {
		panic(fmt.Errorf("wrong argument! try 'metadata-tool show/clean'"))
	}

	switch os.Args[1] {
	case "show":
		show(conn)
		break
	case "clean":
		clean(conn)
		break
	default:
		panic(fmt.Errorf("unknow opration: %s", os.Args[1]))

	}

}
