package main

import (
	"container/list"
	"net"
)

func main() {
	store := Store{store: make(map[string]RedisObject), listClientQueue: make(map[string]*list.List)}
	handler := Handler{Store: &store, ClientCommandQueue: make(map[net.Conn][]Command)}
	server := Server{Port: "6379", Parser: Parser{}, Handler: &handler, connSet: make(map[net.Conn]bool), joinChan: make(chan net.Conn), leaveChan: make(chan net.Conn)} //intialized with default listening port number
	server.StartServer()
}
