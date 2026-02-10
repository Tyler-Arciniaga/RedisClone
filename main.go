package main

import (
	"container/list"
	"net"
)

func main() {
	store := Store{store: make(map[string]RedisObject), listClientQueue: make(map[string]*list.List)}

	handler := Handler{Store: &store, ClientCommandQueue: make(map[net.Conn][]Command)}

	commandBacklog := CommandBacklog{capacity: 100, size: 0, writeHead: 0, backlogStart: 0, Backlog: make([]byte, 100)}

	clientConnSet := NewSafeMap[net.Conn, bool]()
	replicaPortMap := NewSafeMap[string, net.Conn]()
	replicaSet := NewSafeMap[net.Conn, bool]()
	inProgReplicaMap := NewSafeMap[net.Conn, chan ([]byte)]()

	server := Server{
		LocalPort:      "6379",
		Parser:         Parser{},
		Handler:        &handler,
		clientConnSet:  clientConnSet,
		commandBuffer:  []byte{},
		commandBacklog: commandBacklog,
		replicaSet:     replicaSet,
		replPortMap:    replicaPortMap,
		inProgReplMap:  inProgReplicaMap,
		joinChan:       make(chan net.Conn),
		leaveChan:      make(chan net.Conn)} //intialized with default listening port number

	server.StartServer()
}
