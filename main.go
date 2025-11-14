package main

import (
	"bufio"
	"fmt"
	"log/slog"
	"net"
	"os"
)

func main() {
	StartServer()
}

// Bind to port, start new tcp server, and listen for client connections
func StartServer() {
	ln, err := net.Listen("tcp", ":9090") //binds to port localhost 9090
	if err != nil {
		slog.Error(err.Error())
		os.Exit(1)
	}

	for {
		conn, err := ln.Accept()
		if err != nil {
			slog.Error(err.Error())
			continue
		}

		go HandleClientConnection(conn)
	}
}

func HandleClientConnection(conn net.Conn) {
	bufReader := bufio.NewReader(conn)
	for {
		bytes, err := bufReader.ReadBytes('\n')
		msg := string(bytes)
		fmt.Println(msg)
		if err != nil {
			slog.Error("Client error", "error", err)
			continue
		}

		switch msg {
		case "Ping\n":
			conn.Write([]byte("Server: Pong\n"))
		default:
			conn.Write([]byte(fmt.Sprintf("Server: %s", string(bytes))))
		}
	}
}
