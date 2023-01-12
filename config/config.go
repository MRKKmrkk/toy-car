package config

import (
	"encoding/json"
	"io/ioutil"
)

type Config struct {
	Segment struct {
		Store struct {
			MaxBytes uint64
		}
		Index struct {
			MaxBytes uint64
		}
	}
}

func NewConfig(path string) (*Config, error) {

	content, err := ioutil.ReadFile(path)
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
