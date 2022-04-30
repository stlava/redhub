package redhub

import "sync"

type ByteQueue struct {
	sync.Mutex
	items [][]byte
}

func (q *ByteQueue) Push(item []byte) {
	q.Lock()
	defer q.Unlock()
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
