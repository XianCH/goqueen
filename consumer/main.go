package main

import (
	"bytes"
	"fmt"
	"net"

	"github.com/x14n/goExperimental/goQueen/broker"
)

type Comsumer struct {
	Conn net.Conn
}

func NewComsuer(conn net.Conn) *Comsumer {
	return &Comsumer{Conn: conn}
}

func RegistComsumer(addr string) (*Comsumer, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}

	return NewComsuer(conn), nil

}

func (c *Comsumer) ComsumeOne(msg broker.Msg) {
	_, err := c.Conn.Write(broker.MsgToBytes(msg))
	if err != nil {
		fmt.Println("write failed err :", err)
		return
	}
	var res [128]byte
	c.Conn.Read(res[:])
	buf := bytes.NewBuffer(res[:])
	receMsg := broker.BytesToMsg(buf)
	fmt.Println(receMsg)
	c.Conn.Close()
}

func (c *Comsumer) ContinueComue(msg broker.Msg) {

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

func main() {
}
