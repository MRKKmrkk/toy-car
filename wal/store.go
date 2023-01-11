package wal

import (
	"bufio"
	"encoding/binary"
	"os"
	"sync"
)

var (
	// the coder of the binary
	enc = binary.BigEndian
)

const (
	LEN_WIDTH = 8
)

// struct store  maintain a file whcih use to store record
// store makes this file is append-only
type store struct {
	file *os.File
	mu   sync.Mutex
	size uint64
	buf  *bufio.Writer
}

// create a store instance with specific file
func NewStore(file *os.File) (*store, error) {

	info, err := os.Stat(file.Name())
	if err != nil {
		return nil, err
	}

	return &store{
		file: file,
		size: uint64(info.Size()),
		buf:  bufio.NewWriter(file),
	}, nil

}

// append bytes to the file
// returns: (len uint64, pos uint64, err error)
func (s *store) Append(content []byte) (uint64, uint64, error) {

	s.mu.Lock()
	defer s.mu.Unlock()

	pos := s.size

	// wirte length of content into file
	err := binary.Write(s.buf, enc, uint64(len(content)))
	if err != nil {
		return 0, 0, err
	}

	n, err := s.buf.Write(content)
	if err != nil {
		return 0, 0, err
	}
	s.size += uint64(len(content)) + LEN_WIDTH

	return uint64(n) + LEN_WIDTH, pos, nil

}

// read bytes from the pos
// returns: (content []byte, err error)
func (s *store) Read(pos uint64) ([]byte, error) {

	s.mu.Lock()
	defer s.mu.Unlock()

	// flush first
	err := s.buf.Flush()
	if err != nil {
		return nil, err
	}

	// read size
	width := make([]byte, LEN_WIDTH)
	_, err = s.file.ReadAt(width, int64(pos))
	if err != nil {
		return nil, err
	}

	// read file
	content := make([]byte, enc.Uint64(width))
	_, err = s.file.ReadAt(content, int64(pos+LEN_WIDTH))
	if err != nil {
		return nil, err
	}

	return content, nil

}

// flush the buffer and close the file
func (s *store) Close() error {

	s.mu.Lock()
	defer s.mu.Unlock()

	err := s.buf.Flush()
	if err != nil {
		return err
	}

	return s.file.Close()

}

// get name of the file which struct store maintained
func (s *store) Name() string {

	return s.file.Name()

}
