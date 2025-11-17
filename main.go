package main

import (
	"net"
)

func main() {
	s := Server{connSet: make(map[net.Conn]bool), joinChan: make(chan net.Conn)}
	s.StartServer()
}
