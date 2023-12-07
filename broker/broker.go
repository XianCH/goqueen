package broker

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
)

// 初始化broker空间
type Broker struct {
	topic        map[string]*QueueLinkedList
	mu           *sync.RWMutex
	ProducerChan chan Msg
	CosumerChan  chan Msg
}

func NewBroker() *Broker {
	return &Broker{
		topic:        make(map[string]*QueueLinkedList),
		ProducerChan: make(chan Msg, 10),
		CosumerChan:  make(chan Msg, 10),
		mu:           &sync.RWMutex{},
	}
}

func (b *Broker) BrokerChanProcess(conn net.Conn) {
	for {
		select {
		case msgFromProducer := <-b.ProducerChan:
			b.mu.Lock()
			_, exit := b.topic[msgFromProducer.Topic]
			if !exit {
				b.topic[msgFromProducer.Topic] = NewQeueLiskedList()
			}
			list := b.topic[msgFromProducer.Topic]
			list.PushHeader(MsgToBytes(msgFromProducer))
			b.mu.Unlock()

		case msgFromComsumer := <-b.CosumerChan:
			b.mu.RLock()
			if _, exit := b.topic[msgFromComsumer.Topic]; !exit {
				//conn write error msg
				conn.Close()
			}
			list := b.topic[msgFromComsumer.Topic]
			data, err := list.PeckTail()
			if err != nil {
				// write error msg to conn,and break conn
				conn.Close()
			}
			conn.Write(data)
			list.DeleteTail()
			b.mu.RUnlock()
		}
	}
}

func (b *Broker) Process(conn net.Conn) {
	defer handleErr(conn)
	b.BrokerChanProcess(conn)
}

func handleErr(conn net.Conn) {
	if err := recover(); err != nil {
		println(err.(string))
		conn.Write(MsgToBytes(Msg{MsgType: 4}))
	}
}

func BytesToMsg(reader io.Reader) Msg {
	m := Msg{}

	binary.Read(reader, binary.LittleEndian, &m.Id)

	binary.Read(reader, binary.LittleEndian, &m.TopicLen)

	topicBytes := make([]byte, m.TopicLen)
	if _, err := io.ReadFull(reader, topicBytes); err != nil {
		fmt.Println("read topic failed, err:", err)
		return m
	}
	m.Topic = string(topicBytes)

	binary.Read(reader, binary.LittleEndian, &m.MsgType)

	binary.Read(reader, binary.LittleEndian, &m.Len)

	if m.Len <= 0 {
		return m
	}

	m.Payload = make([]byte, m.Len)
	if _, err := io.ReadFull(reader, m.Payload); err != nil {
		fmt.Println("read payload failed, err:", err)
	}

	return m
}

func MsgToBytes(msg Msg) []byte {
	msg.TopicLen = int64(len([]byte(msg.Topic)))
	msg.Len = int64(len([]byte(msg.Payload)))

	var data []byte
	buf := bytes.NewBuffer([]byte{})
	binary.Write(buf, binary.LittleEndian, msg.Id)
	data = append(data, buf.Bytes()...)

	buf = bytes.NewBuffer([]byte{})
	binary.Write(buf, binary.LittleEndian, msg.TopicLen)
	data = append(data, buf.Bytes()...)

	data = append(data, []byte(msg.Topic)...)

	buf = bytes.NewBuffer([]byte{})
	binary.Write(buf, binary.LittleEndian, msg.MsgType)
	data = append(data, buf.Bytes()...)

	buf = bytes.NewBuffer([]byte{})
	binary.Write(buf, binary.LittleEndian, msg.Len)
	data = append(data, buf.Bytes()...)
	data = append(data, []byte(msg.Payload)...)

	return data
}

// func Save() {
// 	ticker := time.NewTicker(60)
// 	for {
// 		select {
// 		case <-ticker.C:
// 			topics.Range(func(key, value interface{}) bool {
// 				if value == nil {
// 					return false
// 				}
// 				file, _ := os.Open(key.(string))
// 				if file == nil {
// 					file, _ = os.Create(key.(string))
// 				}
// 				for msg := value.(*Queue).data.Front(); msg != nil; msg = msg.Next() {
// 					file.Write(MsgToBytes(msg.Value.(Msg)))
// 				}
// 				file.Close()
// 				return false
// 			})
// 		default:
// 			time.Sleep(1)
// 		}
// 	}
// }
