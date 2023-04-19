package main

import (
	"fmt"
	"os"
	"strings"
	api "toy-car/api/v1"
	"toy-car/config"
	"toy-car/wal"
	"toy-car/zookeeper"

	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "toy-car-topic",
	Short: "",
}

func panicIfNotNon(err error) {

	if err != nil {
		panic(err)
	}

}

func getRichZookeeperConnection(zookeeperUrls []string) (*zookeeper.RichZookeeperConnection, error) {

	conf := &config.Config{}
	conf.Server.ZookeeperConnects = zookeeperUrls

	return zookeeper.NewRichZookeeperConnection(conf)

}

func create(zookeeperUrls []string, topic string, partitions uint64, replications uint32) error {

	conn, err := getRichZookeeperConnection(zookeeperUrls)
	if err != nil {
		return err
	}

	topicMetaData := &api.TopicMetaData{
		Version: 1,
	}
	topicMetaData.Partitions = make(map[int32]*api.BrokerIds)

	for i := uint64(0); i < partitions; i++ {

		// allocate repplicates to topic
		ids, err := wal.AllocateReplicaByPolicy(conn, int(replications))
		if err != nil {
			return err
		}

		topicMetaData.Partitions[int32(i)] = &api.BrokerIds{}
		for _, v := range ids {
			topicMetaData.Partitions[int32(i)].BrokerId = append(topicMetaData.Partitions[int32(i)].BrokerId, int32(v))
		}

		// log parititon state in zookeeper
		err = wal.RegistParititionStateMetaData(conn, topic, int(i), ids)
		if err != nil {
			return err
		}

	}

	// log topic information in zookeeper
	err = wal.RegistTopicMetaData(conn, topic, topicMetaData)
	if err != nil {
		return err
	}

	if err != nil {
		return err
	}

	fmt.Printf("created topic %s\n", topic)
	return nil

}

var createCmd = &cobra.Command{
	Use:   "create",
	Short: "Create a new topic",
	Args:  cobra.NoArgs,
	Run: func(cmd *cobra.Command, args []string) {
		zookeeperURL, err := cmd.Flags().GetString("zookeeper")
		panicIfNotNon(err)

		topicName, err := cmd.Flags().GetString("topic")
		panicIfNotNon(err)

		partitions, err := cmd.Flags().GetUint64("partitions")
		panicIfNotNon(err)

		replicationFactor, err := cmd.Flags().GetUint32("replication-factor")
		panicIfNotNon(err)

		urls := strings.Split(zookeeperURL, ",")
		panicIfNotNon(create(urls, topicName, partitions, replicationFactor))

	},
}

func list(zookeeperUrls []string) error {

	conn, err := getRichZookeeperConnection(zookeeperUrls)
	if err != nil {
		return err
	}

	topics, err := conn.ListTopics()
	for _, topic := range topics {
		fmt.Println(topic)
	}

	return nil

}

var listCmd = &cobra.Command{
	Use:   "list",
	Short: "List all of topics",
	Args:  cobra.NoArgs,
	Run: func(cmd *cobra.Command, args []string) {
		zookeeperURL, err := cmd.Flags().GetString("zookeeper")
		panicIfNotNon(err)

		panicIfNotNon(list(strings.Split(zookeeperURL, ",")))

	},
}

func describe(zookeeperUrls []string, topic string) error {

	conn, err := getRichZookeeperConnection(zookeeperUrls)
	if err != nil {
		return nil
	}

	metaData, err := conn.GetTopicMetadata(topic)
	if err != nil {
		return err
	}

	fmt.Printf("describe topic of %s:\n", topic)
	for pid, _ := range metaData.Partitions {
		state, err := conn.GetParititionState(topic, int(pid))
		if err != nil {
			return err
		}

		fmt.Printf("pid = %d, isr = %v, leader id = %d\n", pid, state.ISR, state.Leader)
	}

	return nil

}

var describeCmd = &cobra.Command{
	Use:   "describe",
	Short: "Print details of specific topic",
	Args:  cobra.NoArgs,
	Run: func(cmd *cobra.Command, args []string) {
		zookeeperURL, err := cmd.Flags().GetString("zookeeper")
		panicIfNotNon(err)

		topic, err := cmd.Flags().GetString("topic")
		panicIfNotNon(err)

		panicIfNotNon(describe(strings.Split(zookeeperURL, ","), topic))

	},
}

func init() {
	createCmd.Flags().String("zookeeper", "", "Zookeeper URL for connecting to the cluster")
	createCmd.Flags().String("topic", "", "Name of topic")
	createCmd.Flags().Uint64("partitions", 0, "Number of partitions for the new topic")
	createCmd.Flags().Uint32("replication-factor", 0, "Replication factor for the new topic")
	createCmd.MarkFlagRequired("zookeeper")
	createCmd.MarkFlagRequired("topic")
	createCmd.MarkFlagRequired("partitions")
	createCmd.MarkFlagRequired("replication-factor")
	rootCmd.AddCommand(createCmd)

	listCmd.Flags().String("zookeeper", "", "Zookeeper URL for connecting to the cluster")
	listCmd.MarkFlagRequired("zookeeper")
	rootCmd.AddCommand(listCmd)

	describeCmd.Flags().String("topic", "", "Name of topic")
	describeCmd.MarkFlagRequired("topic")
	describeCmd.Flags().String("zookeeper", "", "Zookeeper URL for connecting to the cluster")
	describeCmd.MarkFlagRequired("zookeeper")
	rootCmd.AddCommand(describeCmd)

}

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
