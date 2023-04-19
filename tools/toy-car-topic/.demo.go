package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "toy-car-topic",
	Short: "",
}

var createCmd = &cobra.Command{
	Use:   "create",
	Short: "Create a new topic",
	Args:  cobra.NoArgs,
	Run: func(cmd *cobra.Command, args []string) {
		zookeeperURL, _ := cmd.Flags().GetString("zookeeper")
		topicName, _ := cmd.Flags().GetString("topic")
		partitions, err := cmd.Flags().GetUint64("partitions")
		if err != nil {
			fmt.Println(err)
		}

		replicationFactor, err := cmd.Flags().GetUint32("replication-factor")
		if err != nil {
			fmt.Println(err)
		}

		fmt.Printf("url = %s, topic = %s, partitions = %d, rep = %d\n", zookeeperURL, topicName, partitions, replicationFactor)

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
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
