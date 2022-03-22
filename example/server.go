package main

import (
	"flag"
	"fmt"
	"log"
	"strings"
	"sync"

	"net/http"
	_ "net/http/pprof"

	"github.com/IceFireDB/redhub"
	"github.com/IceFireDB/redhub/pkg/resp"
)

func main() {
	var mu sync.RWMutex
	var items = make(map[string][]byte)
	var network string
	var addr string
	var multicore bool
	var reusePort bool
	var pprofDebug bool
	var pprofAddr string
	flag.StringVar(&network, "network", "tcp", "server network (default \"tcp\")")
	flag.StringVar(&addr, "addr", "127.0.0.1:6380", "server addr (default \":6380\")")
	flag.BoolVar(&multicore, "multicore", true, "multicore")
	flag.BoolVar(&reusePort, "reusePort", false, "reusePort")
	flag.BoolVar(&pprofDebug, "pprofDebug", false, "open pprof")
	flag.StringVar(&pprofAddr, "pprofAddr", ":8888", "pprof address")
	flag.Parse()
	if pprofDebug {
		go func() {
			http.ListenAndServe(pprofAddr, nil)
		}()
	}

	protoAddr := fmt.Sprintf("%s://%s", network, addr)
	option := redhub.Options{
		Multicore: multicore,
		ReusePort: reusePort,
	}

	signal := make(chan error)

	rh := redhub.NewRedHub(
		func(c redhub.Conn) (action redhub.Action) {
			return
		},
		func(c redhub.Conn, err error) (action redhub.Action) {
			return
		},
		func(c redhub.Conn, cmd resp.Command) (action redhub.Action) {
			var status redhub.Action
			switch strings.ToLower(string(cmd.Args[0])) {
			default:
				c.WriteError("ERR unknown command '" + string(cmd.Args[0]) + "'")
			case "ping":
				c.WriteString("PONG")
			case "quit":
				c.WriteString("OK")
				status = redhub.Close
			case "set":
				if len(cmd.Args) != 3 {
					c.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
					break
				}
				mu.Lock()
				items[string(cmd.Args[1])] = cmd.Args[2]
				mu.Unlock()
				c.WriteString("OK")
			case "get":
				if len(cmd.Args) != 2 {
					c.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
					break
				}
				mu.RLock()
				val, ok := items[string(cmd.Args[1])]
				mu.RUnlock()
				if !ok {
					c.WriteNull()
				} else {
					c.WriteBulk(val)
				}
			case "del":
				if len(cmd.Args) != 2 {
					c.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
					break
				}
				mu.Lock()
				_, ok := items[string(cmd.Args[1])]
				delete(items, string(cmd.Args[1]))
				mu.Unlock()
				if !ok {
					c.WriteInt64(0)
				} else {
					c.WriteInt64(1)
				}
			}
			return status
		},
	)

	go func() {
		select {
		case _ = <-signal:
			fmt.Println("signal recieved")
		}
	}()

	err := redhub.ListendAndServe(signal, protoAddr, option, rh)
	if err != nil {
		log.Fatal(err)
	}
}
