package main

import (
	logger "log"
	"os"
	"strconv"
	"toy-car/config"
	"toy-car/server"
)

func throw(err error) {

	if err != nil {
		panic(err)
	}

}

func main() {

	num, err := strconv.Atoi(os.Args[1])
	throw(err)
	logger.Println("wrong arguments! please try: muids-tool isdNum")

	port := 9998
	for i := 1; i < num+1; i++ {

		go func(id int) {
			logger.Println("init broker")
			c, err := config.NewConfig()
			c.Server.ListenerPort = strconv.FormatInt(int64(port+id), 10)
			c.Server.BrokerId = int64(id)
			throw(err)

			logger.Println("create broker")
			broker, err := server.NewBroker(c)
			throw(err)

			logger.Printf("start up broker on %d", c.Server.BrokerId)
			err = broker.StartUp()
			throw(err)

			for {
			}
		}(i)

	}

	for {
	}

}
