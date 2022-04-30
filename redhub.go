package redhub

import (
	"bytes"
	"errors"
	"github.com/IceFireDB/redhub/pool"
	"sync"
	"time"

	"github.com/IceFireDB/redhub/pkg/resp"
	gnet "github.com/panjf2000/gnet/v2"
)

type Action int

const (
	// None indicates that no action should occur following an event.
	None Action = iota

	// Close closes the connection.
	Close
)

type Options struct {
	// Multicore indicates whether the server will be effectively created with multi-cores, if so,
	// then you must take care with synchronizing memory between all event callbacks, otherwise,
	// it will run the server with single thread. The number of threads in the server will be automatically
	// assigned to the value of logical CPUs usable by the current process.
	Multicore bool

	// LockOSThread is used to determine whether each I/O event-loop is associated to an OS thread, it is useful when you
	// need some kind of mechanisms like thread local storage, or invoke certain C libraries (such as graphics lib: GLib)
	// that require thread-level manipulation via cgo, or want all I/O event-loops to actually run in parallel for a
	// potential higher performance.
	LockOSThread bool

	// ReadBufferCap is the maximum number of bytes that can be read from the client when the readable event comes.
	// The default value is 64KB, it can be reduced to avoid starving subsequent client connections.
	//
	// Note that ReadBufferCap will be always converted to the least power of two integer value greater than
	// or equal to its real amount.
	ReadBufferCap int

	// LB represents the load-balancing algorithm used when assigning new connections.
	LB gnet.LoadBalancing

	// NumEventLoop is set up to start the given number of event-loop goroutine.
	// Note: Setting up NumEventLoop will override Multicore.
	NumEventLoop int

	// ReusePort indicates whether to set up the SO_REUSEPORT socket option.
	ReusePort bool

	// Ticker indicates whether the ticker has been set up.
	Ticker bool

	// TCPKeepAlive sets up a duration for (SO_KEEPALIVE) socket option.
	TCPKeepAlive time.Duration

	// TCPNoDelay controls whether the operating system should delay
	// packet transmission in hopes of sending fewer packets (Nagle's algorithm).
	//
	// The default is true (no delay), meaning that data is sent
	// as soon as possible after a Write.
	TCPNoDelay gnet.TCPSocketOpt

	// SocketRecvBuffer sets the maximum socket receive buffer in bytes.
	SocketRecvBuffer int

	// SocketSendBuffer sets the maximum socket send buffer in bytes.
	SocketSendBuffer int
}

func NewRedHub(
	onOpened func(c Conn) (action Action),
	onClosed func(c Conn, err error) (action Action),
	handler func(c Conn, cmd resp.Command) (action Action),
) *RedHub {
	return &RedHub{
		conns:    make(map[gnet.Conn]*conn),
		connSync: sync.RWMutex{},
		onOpened: onOpened,
		onClosed: onClosed,
		handler:  handler,
	}
}

type RedHub struct {
	*gnet.Engine
	onOpened func(c Conn) (action Action)
	onClosed func(c Conn, err error) (action Action)
	handler  func(c Conn, cmd resp.Command) (action Action)
	conns    map[gnet.Conn]*conn
	connSync sync.RWMutex
	adder    string
	signal   chan error
}

func (rs *RedHub) OnTick() (delay time.Duration, action gnet.Action) {
	return
}

func (rs *RedHub) OnShutdown(eng gnet.Engine) {

}

type connBuffer struct {
	buf     bytes.Buffer
	command []resp.Command
	mu      *sync.Mutex
	pb      *pool.BytePool
	ip      *pool.IntPool
}

func (rs *RedHub) OnOpen(c gnet.Conn) (out []byte, action gnet.Action) {
	rs.connSync.Lock()
	defer rs.connSync.Unlock()

	newConn := NewConn(c)
	rs.conns[c] = newConn

	go newConn.process(rs.handler)

	rs.onOpened(newConn)
	return
}

func (rs *RedHub) OnClose(gc gnet.Conn, err error) (action gnet.Action) {
	rs.connSync.Lock()
	defer rs.connSync.Unlock()

	c := rs.conns[gc]
	if c == nil {
		return
	}
	delete(rs.conns, gc)
	rs.onClosed(c, err)

	_ = c.close()
	return
}

func (rs *RedHub) OnTraffic(gc gnet.Conn) (action gnet.Action) {
	rs.connSync.RLock()
	defer rs.connSync.RUnlock()
	c, ok := rs.conns[gc]
	if !ok {
		return
	}

	c.muClosed.Lock()
	defer c.muClosed.Unlock()

	if c.closed {
		return
	}

	c.cb.mu.Lock()

	buf, _ := gc.Next(-1)
	c.cb.buf.Write(buf)

	// Make sure to make a copy buffer because it's unsafe to reuse across
	// executions.
	target := len(c.cb.buf.Bytes())

	var raw []byte
	if target > pool.BytePoolArrSize {
		raw = make([]byte, target)
	} else {
		raw = c.cb.pb.Get()[0:target]
	}

	copy(raw, c.cb.buf.Bytes())

	// Parse commands
	cmds, lastbyte, err := resp.ReadCommands(c.cb.ip, raw)
	defer c.cb.ip.Reset()

	if err != nil {
		_, _ = gc.Write(resp.AppendError([]byte{}, "ERR "+err.Error()))
		c.cb.mu.Unlock()
		return
	}

	c.cb.command = append(c.cb.command, cmds...)
	c.cb.buf.Reset()
	if len(lastbyte) == 0 {
		c.cb.mu.Unlock()
		c.notify()
	} else {
		c.cb.buf.Write(lastbyte)
		c.cb.mu.Unlock()
	}

	return
}

func (rs *RedHub) OnBoot(srv gnet.Engine) (action gnet.Action) {
	rs.signal <- nil
	return
}

func ListendAndServe(signal chan error, addr string, options Options, rh *RedHub) error {
	serveOptions := gnet.Options{
		Multicore:        options.Multicore,
		LockOSThread:     options.LockOSThread,
		ReadBufferCap:    options.ReadBufferCap,
		LB:               options.LB,
		NumEventLoop:     options.NumEventLoop,
		ReusePort:        options.ReusePort,
		Ticker:           options.Ticker,
		TCPKeepAlive:     options.TCPKeepAlive,
		TCPNoDelay:       gnet.TCPDelay,
		SocketRecvBuffer: options.SocketRecvBuffer,
		SocketSendBuffer: options.SocketSendBuffer,
		ReuseAddr:        false,
	}
	rh.signal = signal

	err := gnet.Run(rh, addr, gnet.WithOptions(serveOptions))

	if err != nil {
		signal <- err
	}
	defer close(signal)
	if err == nil {
		return errors.New("context done")
	}
	return err
}
