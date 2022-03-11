package main

type PriorityQueue struct {
	storage []Timestamp
}

func NewPriorityQueue() PriorityQueue {
	return PriorityQueue{
		storage: make([]Timestamp, 0),
	}
}

func (q *PriorityQueue) Push(t Timestamp) {
	q.storage = append(q.storage, t)
}

func (q *PriorityQueue) Pop() {

}
