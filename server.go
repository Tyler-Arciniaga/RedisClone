package main

import (
	"log/slog"
	"net"
	"os"
	"strconv"
)

type Server struct {
	Parser    Parser
	Store     *Store
	connSet   map[net.Conn]bool
	joinChan  chan (net.Conn)
	leaveChan chan (net.Conn)
}

//TODO instead of having a generate nil string function or using generate bulk string for an "OK" response, just have they pre-made before hand maybe in a map and then use them multiple times
//TODO improve error handling

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

func (s *Server) HandleClientStream(conn net.Conn) {
	buf := make([]byte, 4096)
	temp := make([]byte, 4096)

	for {
		n, err := conn.Read(buf)
		if err != nil {
			slog.Error(err.Error())
			s.leaveChan <- conn
			return
		}

		buf = append(buf, temp[:n]...)
		cmd, consumed, ok := s.Parser.TryParsingCommand(buf)
		if !ok {
			continue
		}
		buf = buf[consumed:]

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
	case "SET":
		ok := s.Store.SetKeyVal(cmd[1], cmd[2])
		if !ok {
			//TODO handle failed set command
		}

		response = s.GenerateSimpleString([]byte("OK"))
	case "GET":
		v := s.Store.GetKeyVal(cmd[1])
		if v == nil {
			response = s.GenerateNilBulkString()
		} else {
			response = s.GenerateBulkString(v)
		}
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

func (s *Server) GenerateNilBulkString() []byte {
	out := make([]byte, 0)
	out = append(out, '$')
	out = strconv.AppendInt(out, -1, 10)
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
