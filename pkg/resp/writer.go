package resp

const bufferSize = 256 * 1024
const cleanUpBufferAfterUses = 1000

type Writer struct {
	b          []byte
	useCounter int32
}

// NewWriter creates a new RESP writer.
func NewWriter() *Writer {
	buff := [bufferSize]byte{}
	return &Writer{b: buff[0:0], useCounter: 0}
}

// WriteNull writes a null to the client
func (w *Writer) WriteNull() {
	w.b = AppendNull(w.b)
}

// WriteArray writes an array header. You must then write additional
// sub-responses to the client to complete the response.
// For example to write two strings:
//
//	c.WriteArray(2)
//	c.WriteBulk("item 1")
//	c.WriteBulk("item 2")
func (w *Writer) WriteArray(count int) {
	w.b = AppendArray(w.b, count)
}

// WriteBulk writes bulk bytes to the client.
func (w *Writer) WriteBulk(bulk []byte) {
	w.b = AppendBulk(w.b, bulk)
}

// WriteBulkString writes a bulk string to the client.
func (w *Writer) WriteBulkString(bulk string) {
	w.b = AppendBulkString(w.b, bulk)
}

// Buffer returns the unflushed buffer. This is a copy so changes
// to the resulting []byte will not affect the writer.
func (w *Writer) Buffer() []byte {
	tmp := make([]byte, len(w.b))
	copy(tmp, w.b)

	return tmp
}

func (w *Writer) OrigBuffer() []byte {
	return w.b
}

// SetBuffer replaces the unflushed buffer with new bytes.
func (w *Writer) SetBuffer(raw []byte) {
	w.b = w.b[:0]
	w.b = append(w.b, raw...)
}

// Flush writes all unflushed Write* calls to the underlying writer.
func (w *Writer) Flush() {
	w.useCounter += 1

	// release buffer memory
	if w.useCounter >= cleanUpBufferAfterUses {
		buff := [bufferSize]byte{}
		w.b = buff[0:0]
		w.useCounter = 0
	}

	w.b = w.b[:0]
}

// WriteError writes an error to the client.
func (w *Writer) WriteError(msg string) {
	w.b = AppendError(w.b, msg)
}

// WriteString writes a string to the client.
func (w *Writer) WriteString(msg string) {
	w.b = AppendString(w.b, msg)
}

// WriteInt writes an integer to the client.
func (w *Writer) WriteInt(num int) {
	w.WriteInt64(int64(num))
}

// WriteInt64 writes a 64-bit signed integer to the client.
func (w *Writer) WriteInt64(num int64) {
	w.b = AppendInt(w.b, num)
}

// WriteUint64 writes a 64-bit unsigned integer to the client.
func (w *Writer) WriteUint64(num uint64) {
	w.b = AppendUint(w.b, num)
}

// WriteRaw writes raw data to the client.
func (w *Writer) WriteRaw(data []byte) {
	w.b = append(w.b, data...)
}

// WriteAny writes any type to client.
//
//	nil             -> null
//	error           -> error (adds "ERR " when first word is not uppercase)
//	string          -> bulk-string
//	numbers         -> bulk-string
//	[]byte          -> bulk-string
//	bool            -> bulk-string ("0" or "1")
//	slice           -> array
//	map             -> array with key/value pairs
//	SimpleString    -> string
//	SimpleInt       -> integer
//	everything-else -> bulk-string representation using fmt.Sprint()
func (w *Writer) WriteAny(v interface{}) {
	w.b = AppendAny(w.b, v)
}
