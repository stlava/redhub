package main

import (
	"flag"
	"fmt"
	"log"

	"net/http"
	_ "net/http/pprof"

	"github.com/IceFireDB/redhub"
)

func main() {
	var network string
	var addr string
	var multicore bool
	var pprofDebug bool
	var pprofAddr string
	flag.StringVar(&network, "network", "tcp", "server network (default \"tcp\")")
	flag.StringVar(&addr, "addr", ":6382", "server addr (default \":6382\")")
	flag.BoolVar(&multicore, "multicore", true, "multicore")
	flag.BoolVar(&pprofDebug, "pprofDebug", false, "open pprof")
	flag.StringVar(&pprofAddr, "pprofAddr", ":8888", "pprof address")
	flag.Parse()
	if pprofDebug {
		go func() {
			http.ListenAndServe(pprofAddr, nil)
		}()
	}

	protoAddr := fmt.Sprintf("%s://%s", network, addr)
	option := redhub.Options{}
	option.Multicore = multicore
	err := redhub.ListendAndServe(protoAddr, option)
	if err != nil {
		log.Fatal(err)
	}
}
