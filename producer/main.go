package main

import (
	"fmt"
	"net"

	"github.com/x14n/goExperimental/goQueen/broker"
)

func produce(msg broker.Msg) {
	conn, err := net.Dial("tcp", "127.0.0.1:12345")
	if err != nil {
		fmt.Print("connect failed, err:", err)
	}
	defer conn.Close()

	n, err := conn.Write(broker.MsgToBytes(msg))
	if err != nil {
		fmt.Print("write failed, err:", err)
	}

	fmt.Print(n)
}

func main() {
	msg := broker.Msg{Id: 1102, Topic: "topic-test", MsgType: 2, Payload: []byte("我")}
	produce(msg)
}
