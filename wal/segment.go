package wal

import (
	"fmt"
	"os"
	"path"
	"toy-car/config"
)

type segment struct {
	index      *index
	store      *store
	baseOffset uint64
	nextOffset uint64
	config     *config.Config
}

func NewSegment(dir string, baseOffset uint64, config *config.Config) (*segment, error) {

	segment := &segment{
		baseOffset: baseOffset,
		config:     config,
	}

	// create store
	fmt.Println(dir, fmt.Sprintf("%d.store", baseOffset))
	storeFile, err := os.OpenFile(
		path.Join(dir, fmt.Sprintf("%d.store", baseOffset)),
		os.O_RDWR|os.O_CREATE|os.O_APPEND,
		0644,
	)
	if err != nil {
		return nil, err
	}

	segment.store, err = NewStore(storeFile)
	if err != nil {
		return nil, err
	}

	// create index
	indexFile, err := os.OpenFile(
		path.Join(dir, fmt.Sprintf("%d.index", baseOffset)),
		os.O_RDWR|os.O_CREATE,
		0644,
	)
	if err != nil {
		return nil, err
	}
	segment.index, err = NewIndex(indexFile, config)
	if err != nil {
		return nil, err
	}

	// figure out nextOffset
	off, _, err := segment.index.Read(-1)
	if err != nil {
		// set baseOffset as nextOffset if index file is empty
		segment.nextOffset = segment.baseOffset
	} else {
		segment.nextOffset = uint64(off + 1)
	}

	return segment, nil

}

// return (offset, err)
func (s *segment) Append(content []byte) (uint64, error) {

	_, pos, err := s.store.Append(content)
	if err != nil {
		return 0, err
	}

	err = s.index.Write(uint32(s.nextOffset-s.baseOffset), pos)
	if err != nil {
		return 0, err
	}

	cur := s.nextOffset
	s.nextOffset++

	return cur, nil

}

func (s *segment) Read(offset uint64) ([]byte, error) {

	_, pos, err := s.index.Read(int64(offset))
	if err != nil {
		return nil, err
	}

	content, err := s.store.Read(pos)
	if err != nil {
		return nil, err
	}

	return content, nil

}

func (s *segment) Close() error {

	err := s.index.Close()
	if err != nil {
		return err
	}

	return s.store.Close()

}

func (s *segment) Remove() error {

	err := s.Close()
	if err != nil {
		return err
	}

	err = os.Remove(s.index.Name())
	if err != nil {
		return err
	}

	return os.Remove(s.store.Name())

}

func (s *segment) IsMax() bool {

	return s.index.size >= s.config.WAL.MaxIndexBytes || s.store.size >= s.config.WAL.MaxStoreBytes

}
