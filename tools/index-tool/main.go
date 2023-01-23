package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

func notError(err error) {

	if err != nil {
		panic(err)
	}

}

func main() {

	var enc = binary.BigEndian

	f, err := os.OpenFile(
		os.Args[1],
		os.O_RDWR,
		0644,
	)
	notError(err)

	arr := make([]byte, 12)
	for {
		_, err := f.Read(arr)
		if err == io.EOF {
			break
		}
		fmt.Printf("pos:%d, off: %d\n", enc.Uint32(arr[:4]), enc.Uint64(arr[4:]))
	}

}
