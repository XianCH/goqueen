package main

import (
	"bytes"
	"fmt"
	"log"
	"net"

	"github.com/x14n/goExperimental/goQueen/broker"
)

type Comsumer struct {
	Conn net.Conn
}

// new comuser struct
func NewComsuer(conn net.Conn) *Comsumer {
	return &Comsumer{Conn: conn}
}

// registe comsuemr client
func RegistComsumer(addr string) (*Comsumer, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}

	return NewComsuer(conn), nil

}

// consume one message
func ComsumeOne(topic string, address string) (*broker.Msg, error) {
	conn, err := net.Dial("tcp", address)
	msg := broker.Msg{
		Topic:   topic,
		MsgType: 1,
	}

	msg1 := broker.MsgToBytes(msg)
	fmt.Println(msg1)
	_, err = conn.Write(msg1)
	if err != nil {
		fmt.Println("write failed err :", err)
		return nil, err
	}
	var res [128]byte
	conn.Read(res[:])
	buf := bytes.NewBuffer(res[:])
	receMsg := broker.BytesToMsg(buf)
	fmt.Println(receMsg)
	conn.Close()
	return &receMsg, nil
}

// continue monitor
func (c *Comsumer) ContinueComue(msg broker.Msg) {
}

func main() {
	address := "127.0.0.1:123456"
	msg, err := ComsumeOne("topic-test", address)
	if err != nil {
		log.Printf("consum err : %s\n", err)
	}

	fmt.Println(msg)

}

// func (c *Comsumer) comsume1() {
// 	conn, err := net.Dial("tcp", "127.0.0.1:12345")
// 	if err != nil {
// 		fmt.Print("connect failed, err:", err)
// 	}
// 	defer conn.Close()

// 	msg := broker.Msg{Topic: "topic-test", MsgType: 1}

// 	n, err := conn.Write(broker.MsgToBytes(msg))
// 	if err != nil {
// 		fmt.Println("write failed, err:", err)
// 	}
// 	fmt.Println("n", n)

// 	var res [128]byte
// 	conn.Read(res[:])
// 	buf := bytes.NewBuffer(res[:])
// 	receMsg := broker.BytesToMsg(buf)
// 	fmt.Print(receMsg)

// 	// ack
// 	conn, _ = net.Dial("tcp", "127.0.0.1:12345")
// 	l, e := conn.Write(broker.MsgToBytes(broker.Msg{Id: receMsg.Id, Topic: receMsg.Topic, MsgType: 3}))
// 	if e != nil {
// 		fmt.Println("write failed, err:", err)
// 	}
// 	fmt.Println("l:", l)
// }
