package main

import (
	"net"
)

func main() {
	store := Store{kvStore: make(map[string]StoreData), listStore: make(map[string]List), listClientQueue: make(map[string][]BlockedPopQueueItem), closedClientChans: make(map[chan [][]byte]bool)}
	handler := Handler{Store: &store}
	server := Server{Parser: Parser{}, Handler: handler, connSet: make(map[net.Conn]bool), joinChan: make(chan net.Conn), leaveChan: make(chan net.Conn)}
	server.StartServer()
}
