package wal

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"toy-car/config"
)

type log struct {
	mu            sync.RWMutex
	dir           string
	config        *config.Config
	activeSegment *segment
	segments      []*segment
}

func (l *log) Dir() string {

	return l.dir

}

func (l *log) newSegment(baseOffset uint64) error {

	s, err := NewSegment(l.Dir(), baseOffset, l.config)
	if err != nil {
		return err
	}

	l.segments = append(l.segments, s)
	l.activeSegment = s

	return nil

}

func NewLog(dir string, config *config.Config) (*log, error) {

	l := &log{
		dir:    dir,
		config: config,
	}

	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	var baseOffsets []uint64
	for _, file := range files {
		offsetStr := strings.TrimSuffix(
			file.Name(),
			path.Ext(file.Name()),
		)

		offset, err := strconv.ParseUint(offsetStr, 10, 0)
		if err != nil {
			return nil, err
		}
		baseOffsets = append(baseOffsets, offset)
	}

	sort.Slice(
		baseOffsets,
		func(i, j int) bool {
			return baseOffsets[i] < baseOffsets[j]
		},
	)

	// create segemnts with baseoffsets
	for i := 0; i < len(baseOffsets); i++ {
		err := l.newSegment(baseOffsets[i])
		if err != nil {
			return nil, err
		}

		// baseoffsets contians dup for index and store
		// so we need plus i twice
		i++
	}

	return l, nil

}

func (l *log) Append(content []byte) (uint64, error) {

	l.mu.Lock()
	defer l.mu.Unlock()

	off, err := l.activeSegment.Append(content)
	if err != nil {
		return 0, err
	}

	if l.activeSegment.IsMax() {
		err = l.newSegment(off + 1)
		if err != nil {
			return 0, err
		}
	}

	return off, nil

}

func (l *log) Read(offset uint64) ([]byte, error) {

	l.mu.RLock()
	defer l.mu.RUnlock()

	var cur *segment
	for _, segment := range l.segments {
		if offset <= segment.baseOffset && offset <= segment.nextOffset {
			cur = segment
			break
		}
	}

	if cur == nil || cur.nextOffset <= offset {
		return nil, fmt.Errorf("offset out of range: %d", offset)
	}

	return cur.Read(offset)

}

func (l *log) Close() error {

	l.mu.Lock()
	defer l.mu.Unlock()

	for _, segment := range l.segments {
		err := segment.Close()
		if err != nil {
			return err
		}
	}

	return nil

}

func (l *log) Remove() error {

	// maybe do not need lock
	//	l.mu.Lock()
	//	defer l.mu.Unlock()

	err := l.Close()
	if err != nil {
		return err
	}

	return os.RemoveAll(l.Dir())

}
