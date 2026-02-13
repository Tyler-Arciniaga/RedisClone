package main

import (
	"container/list"
	"fmt"
	"net"
	"os"
	"sync"
)

func main() {
	port := HandleCommandArgs()
	store := Store{store: make(map[string]RedisObject), listClientQueue: make(map[string]*list.List)}
	handler := Handler{Store: &store, ClientCommandQueue: make(map[net.Conn][]Command)}

	commandBacklog := CommandBacklog{capacity: 100, size: 0, writeHead: 0, backlogStart: 0, Backlog: make([]byte, 100)}

	clientConnSet := NewSafeMap[net.Conn, bool]()
	replicaPortMap := NewSafeMap[string, net.Conn]()
	replicaSet := NewSafeMap[net.Conn, bool]()
	inProgReplicaMap := NewSafeMap[net.Conn, bool]()

	server := Server{
		LocalPort:         port,
		ReplicationID:     "",
		ReplicationOffset: 0,
		Role:              "master",
		MasterPort:        "",
		MasterConn:        nil,

		commandBuffer:  []byte{},
		commandBacklog: commandBacklog,

		clientConnSet: clientConnSet,
		replPortMap:   replicaPortMap,
		replicaSet:    replicaSet,
		inProgReplSet: inProgReplicaMap,

		joinChan:    make(chan net.Conn),
		leaveChan:   make(chan net.Conn),
		HandlerLock: sync.RWMutex{},

		Parser:  &Parser{},
		Handler: &handler,
	}

	server.StartServer()
}

func HandleCommandArgs() string {
	port := "6379"
	for i, v := range os.Args {
		switch v {
		case "--port":
			if i+1 < len(os.Args) {
				port = os.Args[i+1]
			}
		case "--help":
			fmt.Print("This is a multi-threaded Redis clone made entirely in Go!\n\nCommand Flags:\n--port [port number] : to configure the listening port\n--help : You're already here!\n")
			os.Exit(0)
		}
		//TODO handle more command line args eventually
	}

	return port
}
