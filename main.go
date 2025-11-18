package main

import (
	"net"
)

func main() {
	store := Store{store: make(map[string]StoreData)}
	s := Server{Parser: Parser{}, Store: &store, connSet: make(map[net.Conn]bool), joinChan: make(chan net.Conn), leaveChan: make(chan net.Conn)}
	s.StartServer()
}
