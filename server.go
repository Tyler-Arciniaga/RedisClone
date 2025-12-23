package main

import (
	"fmt"
	"log/slog"
	"net"
	"os"
)

type Server struct {
	Parser    Parser
	Handler   Handler
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

	s.Handler.InitalizeHandler()
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
		n, err := conn.Read(temp)
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

func (s *Server) HandleParsedCommands(cmd Command) []byte {
	var response []byte
	switch cmd.Name {
	// Misc Commands
	case "PING":
		response = s.Handler.HandlePingCommand(cmd)
	case "ECHO":
		response = s.Handler.HandleEchoCommand(cmd)
	case "TYPE":
		response = s.Handler.HandleTypeCommand(cmd)
	// String commands
	case "SET":
		response = s.Handler.HandleSetCommand(cmd)
	case "GET":
		response = s.Handler.HandleGetCommand(cmd)
	//List commands
	case "LPUSH":
		response = s.Handler.HandleListPushCommand(cmd)
	case "RPUSH":
		response = s.Handler.HandleListPushCommand(cmd)
	case "LRANGE":
		response = s.Handler.HandleListRangeCommand(cmd)
	case "LLEN":
		response = s.Handler.HandleListLengthCommand(cmd)
	case "LPOP":
		response = s.Handler.HandleListPopCommand(cmd)
	case "RPOP":
		response = s.Handler.HandleListPopCommand(cmd)
	case "BLPOP":
		response = s.Handler.HandleListBlockingPopCommand(cmd)
	case "BRPOP":
		response = s.Handler.HandleListBlockingPopCommand(cmd)
	//Stream commands
	case "XADD":
		response = s.Handler.HandleStreamAdd(cmd)
	case "XLEN":
		response = s.Handler.HandleStreamLen(cmd)
	case "XRANGE":
		response = s.Handler.HandleStreamRange(cmd)
	case "XREVRANGE":
		response = s.Handler.HandleStreamRange(cmd)
	default:
		response = s.Handler.Encoder.GenerateSimpleError(fmt.Sprintf("ERR unknown command '%s'", cmd.Name))
	}

	return response
}
