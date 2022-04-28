package pool

const BytePoolArrSize = 65536

type BytePool struct {
	bp       [][BytePoolArrSize]byte
	bpOffset int
}

func NewBytePool() *BytePool {
	return &BytePool{}
}

func (b *BytePool) Get() []byte {
	if b.bpOffset == len(b.bp) {
		b.bp = append(b.bp, [BytePoolArrSize]byte{})
	}

	r := b.bp[b.bpOffset][0:0]
	b.bpOffset++

	return r
}

func (b *BytePool) Reset() {
	b.bpOffset = 0
}
