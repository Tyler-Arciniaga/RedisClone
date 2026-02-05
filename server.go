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

// all functions pertaining to generic / master server

type Server struct {
	LocalPort         string
	ReplicationID     string
	ReplicationOffset uint64
	Role              string
	MasterPort        string //may be uninitialized if role is master
	MasterConn        net.Conn

	clientConnSet map[net.Conn]bool
	replPortMap   map[string]net.Conn
	inProgReplSet map[net.Conn]bool
	joinChan      chan (net.Conn)
	leaveChan     chan (net.Conn)
	HandlerLock   sync.RWMutex

	Parser  Parser
	Handler *Handler
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
		s.clientConnSet[c] = true
	}
}

func (s *Server) DisconnectConnections() {
	for c := range s.leaveChan {
		delete(s.clientConnSet, c)
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
		resp := s.HandleParsedCommands(cmd, isAtomic, conn)

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
			resp := s.HandleParsedCommands(v, isAtomic, conn) //this function already holds lock on handler thus by setting isAtomic to true it ensures that we don't also try to acquire a RLock (which would cause a deadlock)
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
	infoMap["connected_clients"] = len(s.clientConnSet)
	infoMap["role"] = s.Role
	infoMap["master_replid"] = s.ReplicationID
	infoMap["master_repl_offset"] = s.ReplicationOffset
	return infoMap
}

func (s *Server) HandleParsedCommands(cmd Command, isAtomic bool, conn net.Conn) []byte {
	if !isAtomic {
		s.HandlerLock.RLock()
		defer s.HandlerLock.RUnlock()
	}
	var response []byte

	switch cmd.Name {
	//commands that do not change local data
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
		var repStatus ReplicaRequest
		response, repStatus = s.Handler.HandleReplicaOfCommand(cmd)
		s.HandleReplicaStatus(repStatus)
	case "REPLCONF":
		kvPair := s.Parser.ParseReplConfig(cmd)
		s.HandleReplicaConfig(kvPair, conn)

	//commands that do change local data
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
	default:
		response = s.Handler.Encoder.GenerateSimpleError(fmt.Sprintf("ERR unknown command '%s'", cmd.Name))

	}

	s.IncrementReplicaOffset(cmd)

	return response
}

func (s *Server) IncrementReplicaOffset(cmd Command) {
	writeCommands := []string{"SET", "LPUSH", "RPUSH", "LPOP", "BRPOP", "INCR", "MULTI", "EXEC", "DISCARD"}

	if slices.Contains(writeCommands, cmd.Name) {
		s.ReplicationOffset += uint64(cmd.NumBytes)
	}
}

func (s *Server) HandleReplicaConfig(kvPair []string, conn net.Conn) {
	switch kvPair[0] {
	case "listening-port":
		s.replPortMap[kvPair[1]] = conn
		slog.Info("registered a new replica port", "port", kvPair[1])

		conn.Write(s.Handler.Encoder.GetSimpleStringOk()) //reply to the replica's REPLCONF msg

		//now wait for PSYNC exchange
		bytes := s.WaitForBytes(conn, 10) //wait 10 seconds to recieve PSYNC req
		if bytes == nil {
			return
		}

		psyncReq, _, ok := s.Parser.TryParsingCommand(bytes)
		if !ok {
			slog.Error("recieved invalid Psync request from replicating server")
			return
		}

		psyncResp, needsFullSync := s.Handler.HandlePsyncCommand(psyncReq, s.ReplicationID, s.ReplicationOffset)
		conn.Write(psyncResp)

		if needsFullSync {
			// start background process to send RDB file to replica
			if len(s.inProgReplSet) == 0 {
				//there are currently no replications waiting for a RDB snapshot -> create a new background process to generate an RDB snapshot

				s.inProgReplSet[conn] = true //add current replica conn to set
				returnChan := make(chan ([]byte))
				go s.CreateRDB(returnChan)

				rdb := <-returnChan //blocking in the local thread and waits for RDB to be created

				s.SendRDB(rdb)
			}
		} else {
			// stream the commands that the replica is missing and return
		}
	}
}

func (s *Server) CreateRDB(returnChan chan ([]byte)) {
	var rdb []byte
	rdb = append(rdb, []byte("REDIS")...)
	rdb = append(rdb, []byte("0001")...)
	rdb = append(rdb, 0xFF)

	time.Sleep(5 * time.Second) // placeholder, this mimics the time it might take to generate a new RDB snapshot for the local data

	returnChan <- rdb
}

func (s *Server) SendRDB(rdb []byte) {
	for conn := range s.inProgReplSet {
		conn.Write(rdb)
	}
}

func (s *Server) WaitForBytes(conn net.Conn, n uint64) []byte {
	buf := make([]byte, 4096)

	ctx, cancel := context.WithTimeout(context.TODO(), time.Duration(n)*time.Second) //give the destination server 10 seconds to respond
	defer cancel()

	respChan := make(chan ([]byte))

	go func() {
		n, err := conn.Read(buf)
		if err != nil {
			slog.Error("reading from connection", "err", err)
			respChan <- nil
		} else {
			got := buf[:n]
			respChan <- got
		}
	}()

	select {
	case got := <-respChan:
		if got == nil {
			return nil
		} //error recieving bytes from master connection

		slog.Info("recieved expected bytes!")
		return got

	case <-ctx.Done():
		slog.Error("TIMEOUT server did not recieve expected bytes in time")
		return nil
	}
}
