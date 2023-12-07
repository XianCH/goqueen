package broker

import (
	"log"
	"net"
)

func StartBrokder() {
	broker := NewBroker()

	listen, err := net.Listen("tcp", "127.0.0.1:12345")
	if err != nil {
		log.Printf("StartBrokder err : %s\n", err)
		return
	}

	for {
		conn, err := listen.Accept()
		if err != nil {
			log.Printf("Start Broker err : %s\n", err)
			return
		}
		go broker.Process(conn)
		log.Println("broker server start", listen.Addr(), ".....")
	}
}
