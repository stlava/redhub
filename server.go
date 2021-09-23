package redhub

import (
	"bytes"
	"strings"
	"sync"

	"github.com/IceFireDB/redhub/pkg/resp"
	"github.com/panjf2000/gnet"
)

const (
	// None indicates that no action should occur following an event.
	None Action = iota

	// Close closes the connection.
	Close

	// Shutdown shutdowns the server.
	Shutdown
)

var iceConn map[gnet.Conn]*connBuffer
var connSync sync.RWMutex

type Conn struct {
	gnet.Conn
}

type Action int
type Options struct {
	gnet.Options
}

type redisServer struct {
	*gnet.EventServer
}

type connBuffer struct {
	buf     bytes.Buffer
	command []resp.Command
}

func (rs *redisServer) OnOpened(c gnet.Conn) (out []byte, action gnet.Action) {
	connSync.Lock()
	defer connSync.Unlock()
	iceConn[c] = new(connBuffer)
	return
}

func (rs *redisServer) OnClosed(c gnet.Conn, err error) (action gnet.Action) {
	connSync.Lock()
	defer connSync.Unlock()
	delete(iceConn, c)
	return
}

var mu sync.RWMutex
var items = make(map[string][]byte)

func (rs *redisServer) React(frame []byte, c gnet.Conn) (out []byte, action gnet.Action) {
	connSync.RLock()
	defer connSync.RUnlock()
	cb, ok := iceConn[c]
	if !ok {
		out = resp.AppendError(out, "ERR Client is closed")
		return
	}
	cb.buf.Write(frame)
	cmds, lastbyte, err := resp.ReadCommands(cb.buf.Bytes())
	if err != nil {
		out = resp.AppendError(out, "ERR "+err.Error())
		return
	}
	cb.command = append(cb.command, cmds...)
	cb.buf.Reset()
	if len(lastbyte) == 0 {
		for len(cb.command) > 0 {
			cmd := cb.command[0]
			if len(cb.command) == 1 {
				cb.command = nil
			} else {
				cb.command = cb.command[1:]
			}
			switch strings.ToLower(string(cmd.Args[0])) {
			default:
				out = resp.AppendError(out, "ERR unknown command '"+string(cmd.Args[0])+"'")
			case "ping":
				out = resp.AppendString(out, "PONG")
			case "quit":
				out = resp.AppendString(out, "OK")
				action = gnet.Close
			case "set":
				if len(cmd.Args) != 3 {
					out = resp.AppendError(out, "ERR wrong number of arguments for '"+string(cmd.Args[0])+"' command")
					break
				}
				mu.Lock()
				items[string(cmd.Args[1])] = cmd.Args[2]
				mu.Unlock()
				out = resp.AppendString(out, "OK")
			case "get":
				if len(cmd.Args) != 2 {
					out = resp.AppendError(out, "ERR wrong number of arguments for '"+string(cmd.Args[0])+"' command")
					break
				}
				mu.RLock()
				val, ok := items[string(cmd.Args[1])]
				mu.RUnlock()
				if !ok {
					out = resp.AppendNull(out)
				} else {
					out = resp.AppendBulk(out, val)
				}
			case "del":
				if len(cmd.Args) != 2 {
					out = resp.AppendError(out, "ERR wrong number of arguments for '"+string(cmd.Args[0])+"' command")
					break
				}
				mu.Lock()
				_, ok := items[string(cmd.Args[1])]
				delete(items, string(cmd.Args[1]))
				mu.Unlock()
				if !ok {
					out = resp.AppendInt(out, 0)
				} else {
					out = resp.AppendInt(out, 1)
				}
			case "config":
				// This simple (blank) response is only here to allow for the
				// redis-benchmark command to work with this example.
				out = resp.AppendArray(out, 2)
				out = resp.AppendBulk(out, cmd.Args[2])
				out = resp.AppendBulkString(out, "")
			}
		}
	} else {
		cb.buf.Write(lastbyte)
	}
	return
}

func init() {
	iceConn = make(map[gnet.Conn]*connBuffer)
}

func ListendAndServe(addr string,
	options Options,
) error {
	rs := &redisServer{}
	return gnet.Serve(rs, addr, gnet.WithOptions(options.Options))
}
