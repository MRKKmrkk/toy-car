package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	api "toy-car/api/v1"

	"github.com/golang/protobuf/proto"
)

func main() {
	enc := binary.BigEndian

	f, err := os.OpenFile(
		os.Args[1],
		os.O_RDWR,
		0644,
	)
	if err != nil {
		panic(err)
	}

	lenArr := make([]byte, 8)
	for {
		_, err = f.Read(lenArr)
		if err == io.EOF {
			break
		}
		buf := make([]byte, enc.Uint64(lenArr))

		_, err = f.Read(buf)
		if err == io.EOF {
			break
		}

		msg := &api.Message{}
		proto.Unmarshal(buf, msg)
		fmt.Println(msg)
	}

}
