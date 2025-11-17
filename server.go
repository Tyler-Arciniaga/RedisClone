package main

import (
	"log/slog"
	"net"
	"os"
	"strconv"
)

type Server struct {
	connSet   map[net.Conn]bool
	joinChan  chan (net.Conn)
	leaveChan chan (net.Conn)
}

// Bind to port, start new tcp server, and listen for client connections
func (s *Server) StartServer() {
	ln, err := net.Listen("tcp", ":6379") //binds to port localhost 6379
	if err != nil {
		slog.Error(err.Error())
		os.Exit(1)
	}

	slog.Info("Now listening on port 6793")

	go s.RegisterNewConnections()
	go s.DisconnectConnections()

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

func (s *Server) DisconnectConnections() {
	for c := range s.leaveChan {
		delete(s.connSet, c)
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
				s.leaveChan <- conn
				return
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
					s.leaveChan <- conn
					return
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
					s.leaveChan <- conn
					return
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

		resp := s.HandleParsedCommands(cmd)
		conn.Write(resp)

	}
}

func (s *Server) HandleParsedCommands(cmd [][]byte) []byte {
	var response []byte
	switch string(cmd[0]) {
	case "PING":
		if len(cmd) == 1 {
			response = s.GenerateSimpleString([]byte("PONG"))
		} else {
			response = s.GenerateBulkString(cmd[1])
		}
	case "ECHO":
		response = s.GenerateBulkString(cmd[1])
	}

	return response
}

func (s *Server) GenerateBulkString(bytes []byte) []byte {
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
