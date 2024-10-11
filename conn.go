package redhub

import (
	"context"
	"github.com/IceFireDB/redhub/pkg/resp"
	gnet "github.com/panjf2000/gnet/v2"
	"github.com/panjf2000/gnet/v2/pkg/pool/byteslice"
	"sync"
	"time"
)

var outBufferPool = byteslice.Pool{}

// Conn represents a client connection
type Conn interface {
	// RemoteAddr returns the remote address of the client connection.
	RemoteAddr() string
	// WriteError writes an error to the client.
	WriteError(msg string)
	// WriteString writes a string to the client.
	WriteString(str string)
	// WriteBulk writes bulk bytes to the client.
	WriteBulk(bulk []byte)
	// WriteBulkString writes a bulk string to the client.
	WriteBulkString(bulk string)
	// WriteInt writes an integer to the client.
	WriteInt(num int)
	// WriteInt64 writes a 64-bit signed integer to the client.
	WriteInt64(num int64)
	// WriteUint64 writes a 64-bit unsigned integer to the client.
	WriteUint64(num uint64)
	// WriteArray writes an array header. You must then write additional
	// sub-responses to the client to complete the response.
	// For example to write two strings:
	//
	//   c.WriteArray(2)
	//   c.WriteBulk("item 1")
	//   c.WriteBulk("item 2")
	WriteArray(count int)
	// WriteNull writes a null to the client
	WriteNull()
	// WriteRaw writes raw data to the client.
	WriteRaw(data []byte)
	// WriteAny writes any type to the client.
	//   nil             -> null
	//   error           -> error (adds "ERR " when first word is not uppercase)
	//   string          -> bulk-string
	//   numbers         -> bulk-string
	//   []byte          -> bulk-string
	//   bool            -> bulk-string ("0" or "1")
	//   slice           -> array
	//   map             -> array with key/value pairs
	//   SimpleString    -> string
	//   SimpleInt       -> integer
	//   everything-else -> bulk-string representation using fmt.Sprint()
	WriteAny(any interface{})
	// ReadPipeline returns all commands in current pipeline, if any
	// The commands are removed from the pipeline.
	ReadPipeline() []resp.Command
	// PeekPipeline returns all commands in current pipeline, if any.
	// The commands remain in the pipeline.
	PeekPipeline() []resp.Command
	// GetContext returns the Conn's context
	GetContext() context.Context
	// SetContext sets the Conn's context.
	SetContext(ctx context.Context)
}

const bufferSize = 256 * 1024

var connBufferPool = sync.Pool{
	New: func() interface{} {
		return newConnBuffer()
	},
}

var writerPool = sync.Pool{
	New: func() interface{} {
		return resp.NewWriter()
	},
}

type conn struct {
	conn        gnet.Conn
	cb          *connBuffer
	wr          *resp.Writer
	processData chan interface{}
	closed      bool
	muClosed    *sync.Mutex
	ctx         context.Context
}

func NewConn(gc gnet.Conn) *conn {
	// buffer for read size
	cb := connBufferPool.Get().(*connBuffer)

	return &conn{
		conn:        gc,
		cb:          cb,
		wr:          writerPool.Get().(*resp.Writer),
		processData: make(chan interface{}),
		muClosed:    &sync.Mutex{},
		ctx:         context.Background(),
	}
}

func (c *conn) close() error {
	c.muClosed.Lock()
	defer c.muClosed.Unlock()

	if c.closed {
		return nil
	}

	c.closed = true
	close(c.processData)

	// ensure conn buffer is reset before returning it
	c.cb.reset()
	connBufferPool.Put(c.cb)

	// ensure writer is flushed before returning it
	c.wr.Flush()
	writerPool.Put(c.wr)

	return c.conn.Close()
}

func (c *conn) WriteString(str string)      { c.wr.WriteString(str) }
func (c *conn) WriteBulk(bulk []byte)       { c.wr.WriteBulk(bulk) }
func (c *conn) WriteBulkString(bulk string) { c.wr.WriteBulkString(bulk) }
func (c *conn) WriteInt(num int)            { c.wr.WriteInt(num) }
func (c *conn) WriteInt64(num int64)        { c.wr.WriteInt64(num) }
func (c *conn) WriteUint64(num uint64)      { c.wr.WriteUint64(num) }
func (c *conn) WriteError(msg string)       { c.wr.WriteError(msg) }
func (c *conn) WriteArray(count int)        { c.wr.WriteArray(count) }
func (c *conn) WriteNull()                  { c.wr.WriteNull() }
func (c *conn) WriteRaw(data []byte)        { c.wr.WriteRaw(data) }
func (c *conn) WriteAny(v interface{})      { c.wr.WriteAny(v) }
func (c *conn) RemoteAddr() string          { return c.conn.RemoteAddr().String() }
func (c *conn) ReadPipeline() []resp.Command {
	cmds := c.cb.command
	c.cb.command = []resp.Command{}
	return cmds
}

func (c *conn) PeekPipeline() []resp.Command {
	return c.cb.command
}

func (c *conn) SetContext(ctx context.Context) {
	c.ctx = ctx
}

func (c *conn) GetContext() context.Context {
	return c.ctx
}

func (c *conn) process(handler func(c Conn, cmd resp.Command) (action Action)) {
	for {
		select {
		case _, ok := <-c.processData:
			if !ok {
				return
			}
		}

		status := None

		c.cb.mu.Lock()
		for {
			if len(c.cb.command) == 0 {
				break
			}

			cmd := c.cb.command[0]
			c.cb.command = c.cb.command[1:]

			status = handler(c, cmd)
			if status == Close {
				break
			}
		}

		// Get a buffer out of the pool and if it's big enough use it. Otherwise,
		// allocate a new buffer.
		orig := c.wr.OrigBuffer()
		outBuffer := outBufferPool.Get(len(orig))
		copy(outBuffer, orig)

		c.wr.Flush()
		_ = c.conn.AsyncWrite(outBuffer, func(gc gnet.Conn, _ error) error {
			outBufferPool.Put(outBuffer)
			return nil
		})

		c.cb.pb.Reset()

		if status == Close {
			_ = c.close()
			c.cb.mu.Unlock()
		} else {
			c.cb.lastAccess = time.Now()
			c.cb.mu.Unlock()
		}
	}
}

func (c *conn) notify() {
	c.processData <- struct{}{}
}
