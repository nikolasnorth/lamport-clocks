package main

import "sync"

// Operation represents a bank operation to be performed.
type Operation struct {
	Name          string
	Amount        float32
	AccountNumber int64
}

// Message contains a Lamport timestamp and a bank Operation.
type message struct {
	t  int64
	op Operation
}

// MessageQueue is a thread-safe priority queue.
type MessageQueue struct {
	storage []message
	mutex   sync.Mutex
}

// NewMessageQueue constructs a new empty queue.
func NewMessageQueue() MessageQueue {
	return MessageQueue{
		storage: make([]message, 0),
	}
}

// Push inserts the given message in the correct order.
func (q *MessageQueue) Push(op Operation, t int64) {
	q.mutex.Lock()
	defer q.mutex.Unlock()
}

// Pop returns message, isEmpty. Removes and returns the message with the highest priority, with isEmpty set to false.
// Otherwise, if queue is empty, isEmpty is true.
func (q *MessageQueue) Pop() (Operation, bool) {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	if len(q.storage) == 0 {
		return Operation{}, true
	}

	op := q.storage[0].op
	q.storage = q.storage[1:]
	return op, false
}
