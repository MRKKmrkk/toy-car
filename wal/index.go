package wal

import (
	"fmt"
	"io"
	"os"
	"toy-car/config"

	"github.com/tysonmote/gommap"
)

// struct index maintain a file which use to store index of record
type index struct {
	file *os.File
	size uint64
	mmap gommap.MMap
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

	// apply memory as config.Segment.Store.MaxBytes to map file
	size := info.Size()
	err = os.Truncate(f.Name(), int64(config.WAL.MaxIndexBytes))
	if err != nil {
		return nil, err
	}

	mmap, err := gommap.Map(
		f.Fd(),
		gommap.PROT_READ|gommap.PROT_WRITE,
		gommap.MAP_SHARED,
	)

	return &index{
		size: uint64(size),
		mmap: mmap,
		file: f,
	}, nil

}

func (i *index) Write(offset uint32, position uint64) error {

	i.mmap.Lock()
	defer i.mmap.Unlock()

	// return error if mmap is full
	if len(i.mmap) < int(i.size)+OFFSET_AND_POSITION_WIDTH {
		return io.EOF
	}

	enc.PutUint32(i.mmap[i.size:i.size+OFFSET_WIDTH], offset)
	enc.PutUint64(i.mmap[i.size+OFFSET_WIDTH:i.size+OFFSET_AND_POSITION_WIDTH], position)

	i.size += OFFSET_AND_POSITION_WIDTH
	err := os.Truncate(i.file.Name(), int64(i.size))
	if err != nil {
		return err
	}

	return nil

}

// offset start with 0
// todo: maybe cant change int64 to int32
// return (offset uint32, pos uint64, err error)
func (i *index) Read(offset int64) (uint32, uint64, error) {

	i.mmap.Lock()
	defer i.mmap.Unlock()

	if i.size == 0 {
		return 0, 0, fmt.Errorf("cannt read a empty index file")
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
	if cur >= uint32(i.size) {
		return 0, 0, io.EOF
	}

	off = enc.Uint32(i.mmap[cur : cur+OFFSET_WIDTH])
	return off, enc.Uint64(i.mmap[cur+OFFSET_WIDTH : cur+OFFSET_AND_POSITION_WIDTH]), nil

}

func (i *index) Close() error {

	i.mmap.Lock()
	defer i.mmap.Unlock()

	err := i.mmap.Sync(gommap.MS_SYNC)
	if err != nil {
		return err
	}

	// change index file to real size
	err = os.Truncate(i.file.Name(), int64(i.size))
	if err != nil {
		return err
	}

	return i.file.Close()

}

func (i *index) Name() string {

	return i.file.Name()

}
