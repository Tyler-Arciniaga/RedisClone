package main

import (
	"net"
)

func main() {
	s := Server{Parser: Parser{}, connSet: make(map[net.Conn]bool), joinChan: make(chan net.Conn), leaveChan: make(chan net.Conn)}
	s.StartServer()
}
