package main

import (
	"fmt"
	"log/slog"
	"net"
	"os"
	"strconv"
)

type Server struct {
	connSet  map[net.Conn]bool
	joinChan chan (net.Conn)
}

//TODO handle graceful exits (error EOF)

// Bind to port, start new tcp server, and listen for client connections
func (s *Server) StartServer() {
	ln, err := net.Listen("tcp", ":6379") //binds to port localhost 6379
	if err != nil {
		slog.Error(err.Error())
		os.Exit(1)
	}

	slog.Info("Now listening on port 6793")

	go s.RegisterNewConnections()

	for {
		conn, err := ln.Accept()
		if err != nil {
			slog.Error(err.Error())
			continue
		}

		s.joinChan <- conn
		go s.HandleClientStream(conn)

	}
}

func (s *Server) RegisterNewConnections() {
	for c := range s.joinChan {
		s.connSet[c] = true
	}
}

func (s *Server) ReadLine(buf []byte) ([]byte, int, bool) {
	var line []byte
	for i, v := range buf {
		if v == '\r' && (i+1) < len(buf) && buf[i+1] == '\n' { // delimiter found...
			line = buf[:i] //extract chunk of slice up to but not including delimter
			return line, i, true
		}
	}
	return nil, 0, false
}

func (s *Server) ReadBulkString(l int, buf []byte) ([]byte, int) {
	var bytes []byte
	for i, v := range buf {
		if v == '\r' && (i+1) < len(buf) && buf[i+1] == '\n' { // delimiter found...
			bytes = buf[:i] //extract chunk of slice up to but not including delimter
			return bytes, i
		}
	}

	return nil, 0 //should never be reached
}

func (s *Server) HandleClientStream(conn net.Conn) {
	buf := make([]byte, 1024)
	newChunk := make([]byte, 1024)

	for {
		n, err := conn.Read(buf)
		if err != nil {
			slog.Error(err.Error())
			break
		}
		buf = buf[:n]

		var cmd [][]byte

		//Read up until first delimeter
		line, i, ok := s.ReadLine(buf)
		for !ok {
			//read in new chunk and append to buffer
			n, err := conn.Read(newChunk)
			if err != nil {
				slog.Error(err.Error())
				os.Exit(1) //TODO improve error handling
			}
			buf = append(buf, newChunk[:n]...)
			line, i, ok = s.ReadLine(buf)
		}

		cmdArrayLen := s.ReadPrefixLength(line) //determine the array length

		if i+2 >= len(buf) {
			buf = []byte{}
		} else {
			buf = buf[i+2:]
		}

		for range cmdArrayLen {
			line, i, ok := s.ReadLine(buf)
			for !ok {
				n, err := conn.Read(newChunk)
				if err != nil {
					slog.Error(err.Error())
					os.Exit(1) //TODO improve error handling
				}
				buf = append(buf, newChunk[:n]...)
				line, i, ok = s.ReadLine(buf)
			}

			prefixLen := s.ReadPrefixLength(line) //determine the bulk string length

			if i+2 >= len(buf) {
				buf = []byte{}
			} else {
				buf = buf[i+2:]
			}

			if len(buf) < prefixLen {
				//read in new chunk and append to buffer here
				n, err := conn.Read(newChunk)
				if err != nil {
					slog.Error(err.Error())
					os.Exit(1) //TODO improve error handling
				}
				buf = append(buf, newChunk[:n]...)
			}

			bytes, i := s.ReadBulkString(prefixLen, buf)
			cmd = append(cmd, bytes)

			if i+2 >= len(buf) {
				buf = []byte{}
			} else {
				buf = buf[i+2:]
			}

		}

		fmt.Println("Done parsing!")
		for _, v := range cmd {
			fmt.Println(string(v))
		}
	}
}

// // Redis client always sends command as an array of bulk strings -> cmd, arg1, arg2, ...
// func (s *Server) ParseIncomingCommand(b []byte, conn net.Conn) {
// 	numElements, i := s.ReadCommandLength(b, 1)

// 	var elements [][]byte
// 	for j := 0; j < numElements; j++ {
// 		element, temp := s.ParseElement(b, i)
// 		i = temp
// 		elements = append(elements, element)
// 	}

// 	for i, v := range elements {
// 		fmt.Println(i, string(v))
// 	}

// 	switch string(elements[0]) {
// 	//TODO make sure to serialize return message back into RESP, don't just send as raw bytes without serialization
// 	case "PING":
// 		if len(elements) == 1 {
// 			b := s.GenerateSimpleString([]byte("PONG"))
// 			conn.Write(b)
// 		} else {
// 			b := s.GenerateBulkString(elements[1])
// 			conn.Write(b)
// 		}
// 	}
// }

func (s *Server) GenerateBulkString(bytes []byte) []byte {
	fmt.Println(len(bytes))
	out := make([]byte, 0, len(bytes)+32)
	out = append(out, '$')
	out = strconv.AppendInt(out, int64(len(bytes)), 10)
	out = append(out, '\r', '\n')
	out = append(out, bytes...)
	out = append(out, '\r', '\n')
	return out
}
func (s *Server) GenerateSimpleString(bytes []byte) []byte {
	out := make([]byte, 0, len(bytes)+32)
	out = append(out, '+')
	out = append(out, bytes...)
	out = append(out, '\r', '\n')
	return out
}

// func (s *Server) ParseElement(b []byte, i int) ([]byte, int) {
// 	elementLen, start := s.ReadCommandLength(b, i+1) //TODO account for the data type which is in b[i] first
// 	fmt.Println("here", string(b[start+1:]))
// 	var element []byte
// 	element = append(element, b[start:start+elementLen]...)

//		return element, start + elementLen + 2
//	}
func (s *Server) ReadPrefixLength(b []byte) int {
	//reverse byte slice to make digit math work
	for i, j := 0, len(b)-1; i < j; i, j = i+1, j-1 {
		b[i], b[j] = b[j], b[i]
	}

	var numElements int
	multiplier := 1

	for _, v := range b {
		digit, err := strconv.Atoi(string(v))
		if err != nil {
			continue
		}
		numElements += (digit * multiplier)
		multiplier *= 10
	}
	return numElements
}
