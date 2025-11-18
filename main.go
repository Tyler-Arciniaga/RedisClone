package main

import (
	"net"
)

func main() {
	store := Store{store: make(map[string]StoreData)}
	handler := Handler{Store: &store}
	server := Server{Parser: Parser{}, Handler: handler, connSet: make(map[net.Conn]bool), joinChan: make(chan net.Conn), leaveChan: make(chan net.Conn)}
	server.StartServer()
}
