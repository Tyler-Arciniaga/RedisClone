package main

import (
	"container/list"
	"net"
)

func main() {
	store := Store{store: make(map[string]RedisObject), listClientQueue: make(map[string]*list.List)}

	handler := Handler{Store: &store, ClientCommandQueue: make(map[net.Conn][]Command)}

	cmdBacklog := CommandBacklog{capacity: 100, size: 0, writeHead: 0, backlogStart: 0, Backlog: make([]byte, 100)}

	server := Server{
		LocalPort:      "6379",
		Parser:         Parser{},
		Handler:        &handler,
		clientConnSet:  make(map[net.Conn]bool),
		commandBuffer:  []byte{},
		commandBacklog: cmdBacklog,
		replicaSet:     make(map[net.Conn]bool),
		replPortMap:    make(map[string]net.Conn),
		inProgReplMap:  make(map[net.Conn]chan ([]byte)),
		joinChan:       make(chan net.Conn),
		leaveChan:      make(chan net.Conn)} //intialized with default listening port number

	server.StartServer()
}
