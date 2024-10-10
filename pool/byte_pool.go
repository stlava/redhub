package pool

const BytePoolArrSize = 65536
const cleanUpBytePoolAfterUses = 1000

type BytePool struct {
	bp         [][BytePoolArrSize]byte
	bpOffset   int
	useCounter int32
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

	b.useCounter += 1

	if b.useCounter >= cleanUpBytePoolAfterUses {
		b.useCounter = 0
		b.bp = [][BytePoolArrSize]byte{}
	}
}
