package redhub

import "sync"

const cleanUpQueueAfterUses = 100

type ByteQueue struct {
	useCounter int32
	sync.Mutex
	items [][]byte
}

func (q *ByteQueue) Push(item []byte) {
	q.Lock()
	defer q.Unlock()

	// recycle memory. technically the goal is to do this per item
	// but tracking usage of specific items is non-trivial.
	q.useCounter += 1
	if q.useCounter >= cleanUpQueueAfterUses {
		q.items = make([][]byte, 0)
		q.useCounter = 0
	}

	q.items = append(q.items, item)
}

func (q *ByteQueue) Pop() []byte {
	q.Lock()
	defer q.Unlock()

	if len(q.items) == 0 {
		return make([]byte, 0, bufferSize)
	}

	item := q.items[0]
	q.items = q.items[1:]

	return item
}
