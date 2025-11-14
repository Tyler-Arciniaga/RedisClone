package main

import (
	"fmt"
	"log/slog"
	"net"
	"os"
	"strconv"
)

func main() {
	StartServer()
}

// Bind to port, start new tcp server, and listen for client connections
func StartServer() {
	ln, err := net.Listen("tcp", ":6379") //binds to port localhost 6379
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
	buf := make([]byte, 1024) // Don't make buffered to 1024 bytes as client msg may exceed this
	for {
		n, err := conn.Read(buf)
		if err != nil {
			slog.Error(err.Error())
			continue
		}
		ParseIncomingCommand(buf[:n], conn)

	}
}

func ParseIncomingCommand(b []byte, conn net.Conn) {
	numElements, i := ReadCommandLength(b, 1)
	fmt.Println("Num Elements:", numElements)

	var elements [][]byte
	for j := 0; j < numElements; j++ {
		element, temp := ParseElement(b, i)
		i = temp
		elements = append(elements, element)
	}

	switch string(elements[0]) {
	//TODO make sure to serialize return message back into RESP, don't just send as raw bytes without serialization
	case "PING":
		if len(elements) == 1 {
			conn.Write([]byte("PONG"))
		} else {
			conn.Write(elements[1])
		}
	}
}

func ParseElement(b []byte, i int) ([]byte, int) {
	elementLen, i := ReadCommandLength(b, i+1) //TODO account for the data type which is in b[i] first
	var element []byte
	element = append(element, b[i:i+elementLen]...)
	i += elementLen

	return element, i
}
func ReadCommandLength(b []byte, i int) (int, int) {
	var numElements int
	multiplier := 1
	for i < len(b) && !(string(b[i]) == "\r" && string(b[i+1]) == "\n") {
		digit, _ := strconv.Atoi(string(b[i]))
		numElements += (digit * multiplier)
		multiplier *= 10
		i += 1
	}
	return numElements, i + 2
}
