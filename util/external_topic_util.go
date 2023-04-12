package util

import (
	"bufio"
	"os"
	"path"
	"sync"
	"toy-car/config"
)

type ExternalTopicManager struct {
	file *os.File
	mu   sync.Mutex
	buf  *bufio.Writer
}

func NewExternalTopicManager(config *config.Config) (*ExternalTopicManager, error) {

	// create external topic
	// net init datg yet
	file, err := os.OpenFile(
		path.Join(config.LogDir, "consumer_offsets"),
		os.O_RDWR|os.O_CREATE,
		0644,
	)
	if err != nil {
		return nil, err
	}

	return &ExternalTopicManager{
		file: file,
		buf:  bufio.NewWriter(file),
	}, nil

}

//func (etm *ExternalTopicManager) PutLeo(topic string, pid int) error {
//
//
//
//}
