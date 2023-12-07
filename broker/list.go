package broker

import (
	"errors"
	"sync"
)

type ListNode struct {
	Next *ListNode
	Data []byte
}

type QueueLinkedList struct {
	mu     *sync.Mutex
	Header *ListNode
	Tail   *ListNode
}

func NewQeueLiskedList() *QueueLinkedList {
	return &QueueLinkedList{
		mu:     &sync.Mutex{},
		Header: nil,
		Tail:   nil,
	}
}

func (ql *QueueLinkedList) ListIsEmpty() bool {
	return ql.Header == nil || ql.Tail == nil
}

// push Node to the header
func (ql *QueueLinkedList) PushHeader(data []byte) error {
	ql.mu.Lock()
	defer ql.mu.Unlock()
	newNode := &ListNode{Data: data}
	if ql.ListIsEmpty() {
		ql.Header = newNode
		ql.Tail = newNode
		return nil
	}
	ql.Tail.Next = newNode
	ql.Tail = newNode
	newNode.Next = nil
	return nil
}

func (ql *QueueLinkedList) PeckTail() ([]byte, error) {
	if ql.ListIsEmpty() {
		return nil, errors.New("队列为空")
	}
	return ql.Tail.Data, nil
}

func (ql *QueueLinkedList) DeleteTail() error {
	ql.mu.Lock()
	defer ql.mu.Unlock()
	if ql.ListIsEmpty() {
		return errors.New("队列为空")
	}
	//if only has one node
	if ql.Header.Next == nil {
		ql.Header = nil
		ql.Tail = nil
		return nil
	}

	current := ql.Header
	var prev *ListNode
	for current.Next != nil {
		prev = current
		current = current.Next
	}
	ql.Tail = prev
	prev.Next = nil
	return nil
}
