package main

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"net"
	"os"
	"slices"
	"strings"
	"sync"
	"time"
)

type Server struct {
	LocalPort         string
	ReplicationID     string
	ReplicationOffset int64
	Role              string
	MasterPort        string //may be uninitialized if role is master
	MasterConn        net.Conn

	Parser      Parser
	Handler     *Handler
	connSet     map[net.Conn]bool
	joinChan    chan (net.Conn)
	leaveChan   chan (net.Conn)
	HandlerLock sync.RWMutex
}

func (s *Server) HandleCommandArgs() {
	for i, v := range os.Args {
		switch v {
		case "--port":
			if i+1 < len(os.Args) {
				s.LocalPort = os.Args[i+1]
			}
		case "--help":
			fmt.Print("This is a redis clone made entirely in Go!\n\nCommand Flags:\n--port [port number] : to configure the listening port\n--help : You're already here!\n")
			os.Exit(0)
		}
		//TODO handle more command line args eventually
	}
}

func (s *Server) GenerateReplicationID() string {
	charset := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	var sb strings.Builder
	sb.Grow(40)
	for range 40 {
		sb.WriteByte(charset[rand.Intn(len(charset))])
	}

	return sb.String()
}
func (s *Server) ConfigureMasterStatus() {
	s.Role = "master"
	s.MasterPort = ""
	s.ReplicationID = s.GenerateReplicationID()
	s.ReplicationOffset = 0
	s.MasterConn = nil
}

// Bind to port, start new tcp server, and listen for client connections
func (s *Server) StartServer() {
	// handle command line flags and default to master status (regarding master-replica hierarchy)
	s.HandleCommandArgs()
	s.ConfigureMasterStatus()

	port := fmt.Sprint(":", s.LocalPort)
	ln, err := net.Listen("tcp", port) //binds to port localhost 6379
	if err != nil {
		slog.Error(err.Error())
		os.Exit(1)
	}

	slog.Info(fmt.Sprint("Now listening on port ", s.LocalPort))

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
			slog.Info("A client has disconnected")
			s.leaveChan <- conn
			return
		}

		buf = append(buf, temp[:n]...)
		cmd, consumed, ok := s.Parser.TryParsingCommand(buf)
		if !ok {
			continue
		}

		cmd.NumBytes = int64(consumed)
		buf = buf[consumed:]

		isAtomic := false
		resp := s.HandleParsedCommands(cmd, isAtomic)

		conn.Write(resp)

		if cmd.Name == "MULTI" {
			//enter transaction mode for this client
			s.HandleClientTransaction(conn)
		}

	}
}

func (s *Server) HandleClientTransaction(conn net.Conn) {
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

		switch cmd.Name {
		case "EXEC":
			s.ExecuteTransaction(conn)
			return
		case "DISCARD":
			resp := s.Handler.DiscardCommandQueue(conn)
			conn.Write(resp)
			return
		case "MULTI":
			resp := s.Handler.Encoder.GenerateSimpleError("ERR cannot nest MULTI commands")
			conn.Write(resp)
		default:
			resp := s.Handler.QueueCommand(cmd, conn)
			conn.Write(resp)
		}
	}
}

func (s *Server) ExecuteTransaction(conn net.Conn) {
	s.HandlerLock.Lock() //acquire lock on Handler to ensure that all commands in transaction are handled as one atomic unit
	defer s.HandlerLock.Unlock()

	commandQ := s.Handler.GetCommandQueue(conn)
	if commandQ == nil {
		resp := s.Handler.Encoder.GenerateNilArray()
		conn.Write(resp)
	} else {
		var results [][]byte
		for _, v := range commandQ {
			isAtomic := true
			resp := s.HandleParsedCommands(v, isAtomic) //this function already holds lock on handler thus by setting isAtomic to true it ensures that we don't also try to acquire a RLock (which would cause a deadlock)
			results = append(results, resp)
		}

		isForTransaction := true
		resp := s.Handler.Encoder.GenerateArray(results, isForTransaction) //isForTransaction needed for some formatting input for encoder
		conn.Write(resp)
	}
}

func (s *Server) BundleServerInfo() map[string]any {
	infoMap := make(map[string]any)
	infoMap["tcp_port"] = s.LocalPort
	infoMap["connected_clients"] = len(s.connSet)
	infoMap["role"] = s.Role
	infoMap["master_replid"] = s.ReplicationID
	infoMap["master_repl_offset"] = s.ReplicationOffset
	return infoMap
}

func (s *Server) HandleParsedCommands(cmd Command, isAtomic bool) []byte {
	if !isAtomic {
		s.HandlerLock.RLock()
		defer s.HandlerLock.RUnlock()
	}
	var response []byte

	//commands that do not change data set
	switch cmd.Name {
	case "PING":
		response = s.Handler.HandlePingCommand(cmd)
	case "ECHO":
		response = s.Handler.HandleEchoCommand(cmd)
	case "TYPE":
		response = s.Handler.HandleTypeCommand(cmd)
	case "GET":
		response = s.Handler.HandleGetCommand(cmd)
	case "LRANGE":
		response = s.Handler.HandleListRangeCommand(cmd)
	case "LLEN":
		response = s.Handler.HandleListLengthCommand(cmd)
	case "INFO":
		response = s.Handler.HandleInfoCommand(cmd, s.BundleServerInfo())
	case "REPLICAOF":
		resp, repStatus := s.Handler.HandleReplicaOfCommand(cmd)
		s.HandleReplicaStatus(repStatus)
		response = resp
	default:
		response = s.Handler.Encoder.GenerateSimpleError(fmt.Sprintf("ERR unknown command '%s'", cmd.Name))
	}

	//commands that do change data set
	switch cmd.Name {
	case "SET":
		response = s.Handler.HandleSetCommand(cmd)
	case "LPUSH":
		response = s.Handler.HandleListPushCommand(cmd)
	case "RPUSH":
		response = s.Handler.HandleListPushCommand(cmd)
	case "LPOP":
		response = s.Handler.HandleListPopCommand(cmd)
	case "RPOP":
		response = s.Handler.HandleListPopCommand(cmd)
	case "BLPOP":
		response = s.Handler.HandleListBlockingPopCommand(cmd)
	case "BRPOP":
		response = s.Handler.HandleListBlockingPopCommand(cmd)
	case "INCR":
		response = s.Handler.HandleIncrCommand(cmd)
	case "MULTI":
		response = s.Handler.HandleMultiCommand(cmd)
	case "EXEC":
		response = s.Handler.Encoder.GenerateSimpleError("ERR client is currently not in transaction mode, enter transaction mode with MULTI command")
	case "DISCARD":
		response = s.Handler.Encoder.GenerateSimpleError("ERR client is currently not in transaction mode, enter transaction mode with MULTI command")
	}

	s.HandleReplicaOffset(cmd)

	return response
}

func (s *Server) HandleReplicaStatus(repStatus ReplicaRequest) {
	if repStatus.isNowMaster {
		s.ConfigureMasterStatus()
	} else {
		//TODO!!!: handle hanshake -> PSYNC, etc
		s.EstablishMasterHandshake(repStatus.masterPort)
	}
}

func (s *Server) EstablishMasterHandshake(masterPort string) {
	port := fmt.Sprintf("localhost:%s", masterPort)
	conn, err := net.Dial("tcp", port)
	s.MasterConn = conn
	if err != nil {
		slog.Error(err.Error())
		os.Exit(1)
	}

	s.TestMasterConn()
}

func (s *Server) TestMasterConn() error {
	bytes := s.Handler.Encoder.GeneratePing()

	_, err := s.MasterConn.Write(bytes)
	if err != nil {
		return err
	}

	s.WaitForPong()

	return nil
}

func (s *Server) WaitForPong() bool {
	buf := make([]byte, 4096)

	ctx, cancel := context.WithTimeout(context.TODO(), time.Duration(10)*time.Second) //give the master server 10 seconds to respond to PING
	defer cancel()

	pongChan := make(chan ([]byte))

	go func() {
		n, err := s.MasterConn.Read(buf)
		if err != nil {
			slog.Error("ERROR reading from master server connection")
			pongChan <- nil
		} else {
			got := buf[:n]
			pongChan <- got
		}

	}()

	select {
	case got := <-pongChan:
		if got == nil {
			return false
		} //error recieving bytes from master connection

		expect := s.Handler.Encoder.GenerateSimpleString([]byte("PONG"))
		if eq := slices.Equal(expect, got); !eq {
			slog.Error("ERROR recieved incorrect signal from master server after sending PING")
			return false
		} else {
			slog.Info("recieved PONG signal from master server!")
			return true
		}
	case <-ctx.Done():
		slog.Error("TIMEOUT replica server did not recieve PING response in time")
		return false

	}
}

func (s *Server) HandleReplicaOffset(cmd Command) {
	writeCommands := []string{"SET", "LPUSH", "RPUSH", "LPOP", "BRPOP", "INCR", "MULTI", "EXEC", "DISCARD"}

	if slices.Contains(writeCommands, cmd.Name) {
		s.ReplicationOffset += cmd.NumBytes
	}
}
