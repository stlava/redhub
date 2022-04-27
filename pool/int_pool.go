package pool

const intArrSize = 16384

type IntPool struct {
	bp       [][intArrSize]int
	bpOffset int
}

func NewIntPool() *IntPool {
	return &IntPool{}
}

func (b *IntPool) Get() []int {
	if b.bpOffset == len(b.bp) {
		b.bp = append(b.bp, [intArrSize]int{})
	}

	r := b.bp[b.bpOffset][0:0]
	b.bpOffset++

	return r
}

func (b *IntPool) Reset() {
	b.bpOffset = 0
}
