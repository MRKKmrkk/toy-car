package wal

import (
	"bufio"
	"encoding/binary"
	"io"
	"os"
	"sync"
	"toy-car/config"
)

// struct index maintain a file which use to store index of record
type index struct {
	file     *os.File
	size     uint64
	capacity uint64
	mu       sync.Mutex
	buf      *bufio.Writer
}

const (
	OFFSET_WIDTH              = 4
	POSITON_WIDTH             = 8
	OFFSET_AND_POSITION_WIDTH = 12
)

// create a new index instance with specific file and config
func NewIndex(f *os.File, config *config.Config) (*index, error) {

	info, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}

	return &index{
		size:     uint64(info.Size()),
		capacity: config.Segment.Index.MaxBytes,
		file:     f,
		buf:      bufio.NewWriter(f),
	}, nil

}

func (i *index) Write(offset uint32, position uint64) error {

	i.mu.Lock()
	defer i.mu.Unlock()

	// return error if mmap is full
	if i.capacity < i.size+OFFSET_AND_POSITION_WIDTH {
		return io.EOF
	}

	err := binary.Write(i.buf, enc, offset)
	if err != nil {
		return err
	}
	err = binary.Write(i.buf, enc, position)
	if err != nil {
		return err
	}

	i.size += OFFSET_AND_POSITION_WIDTH

	return nil

}

// offset start with 0
// todo: maybe cant change int64 to int32
// return (offset uint32, pos uint64, err error)
func (i *index) Read(offset int64) (uint32, uint64, error) {

	i.mu.Lock()
	defer i.mu.Unlock()

	err := i.buf.Flush()
	if err != nil {
		return 0, 0, err
	}

	if i.size == 0 {
		return 0, 0, nil
	}

	// -1 repesents the last offset of index
	var off uint32
	if offset == -1 {
		off = (uint32(i.size) / OFFSET_AND_POSITION_WIDTH) - 1
	} else {
		off = uint32(offset)
	}

	// figure out actually position
	cur := off * OFFSET_AND_POSITION_WIDTH
	if cur > uint32(i.capacity) {
		return 0, 0, io.EOF
	}

	buffer := make([]byte, OFFSET_AND_POSITION_WIDTH)
	_, err = i.file.ReadAt(buffer, int64(cur))
	if err != nil {
		return 0, 0, err
	}

	return enc.Uint32(buffer[:OFFSET_WIDTH]), enc.Uint64(buffer[OFFSET_WIDTH:]), nil

}

func (i *index) Close() error {

	i.mu.Lock()
	defer i.mu.Unlock()

	err := i.buf.Flush()
	if err != nil {
		return err
	}

	return i.file.Close()

}

func (i *index) Name() string {

	return i.file.Name()

}
