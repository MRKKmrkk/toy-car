package config

import (
	"encoding/json"
	"io/ioutil"
)

type Config struct {
	LogDir string
	WAL    struct {
		InitOffset    uint64
		MaxIndexBytes uint64
		MaxStoreBytes uint64
	}
	Server struct {
		BrokerId                int64
		ListenerAddress         string
		ListenerPort            string
		ZookeeperConnects       []string
		ZookeeperTimeoutSeconds int
	}
}

func NewConfig() (*Config, error) {

	content, err := ioutil.ReadFile("/root/workspace/github.com/toy-car/config/toy-car.json")
	if err != nil {
		return nil, err
	}

	c := &Config{}
	err = json.Unmarshal(content, c)
	if err != nil {
		return nil, err
	}

	return c, nil

}
