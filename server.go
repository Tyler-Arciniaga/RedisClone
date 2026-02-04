package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"net"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Server struct {
	LocalPort         string
	ReplicationID     string
	ReplicationOffset uint64
	Role              string
	MasterPort        string //may be uninitialized if role is master
	MasterConn        net.Conn

	clientConnSet map[net.Conn]bool
	replPortSet   map[string]bool
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
	infoMap["connected_clients"] = len(s.clientConnSet)
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

	//commands that do not change local data
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
		var repStatus ReplicaRequest
		response, repStatus = s.Handler.HandleReplicaOfCommand(cmd)
		s.HandleReplicaStatus(repStatus)
	case "REPLCONF":
		var kvPair []string
		response, kvPair = s.Handler.HandleReplicaConfigCommand(cmd)
		s.ParseReplicaConfig(kvPair)
	case "PSYNC":
		response = s.Handler.HandlePsyncCommand(cmd, s.ReplicationID, s.ReplicationOffset)
	default:
		response = s.Handler.Encoder.GenerateSimpleError(fmt.Sprintf("ERR unknown command '%s'", cmd.Name))
	}

	//commands that do change local data
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

	s.IncrementReplicaOffset(cmd)

	return response
}

func (s *Server) HandleReplicaStatus(repStatus ReplicaRequest) {
	if repStatus.isNowMaster {
		s.ConfigureMasterStatus()
	} else {
		//TODO!!!: handle hanshake -> PSYNC, etc
		psyncResponse, err := s.EstablishMasterHandshake(repStatus.masterPort)
		if err != nil {
			slog.Error("Handshake with master server failed", "err", err.Error())
			return
		}

		s.HandlePsyncResponse(psyncResponse)
	}
}

func (s *Server) HandlePsyncResponse(resp PsyncResponse) {
	fmt.Println("Got response:", resp)
}

func (s *Server) EstablishMasterHandshake(masterPort string) (PsyncResponse, error) {
	var psyncResp PsyncResponse

	port := fmt.Sprintf("localhost:%s", masterPort)
	conn, err := net.Dial("tcp", port)
	s.MasterConn = conn
	if err != nil {
		return psyncResp, err
	}

	err = s.PingMasterConn()
	if err != nil {
		return psyncResp, err
	}

	//Send REPLCONF signal to register replica listening port with master server
	err = s.SendReplConf()
	if err != nil {
		return psyncResp, err
	}

	psyncResp, err = s.ExchangePsync()
	if err != nil {
		return psyncResp, err
	}

	return psyncResp, nil
}

func (s *Server) ExchangePsync() (PsyncResponse, error) {
	//send PSYNC command
	var psyncResp PsyncResponse
	bytes := s.Handler.Encoder.GeneratePsync(s.ReplicationID, s.ReplicationOffset)
	_, err := s.MasterConn.Write(bytes)
	if err != nil {
		return psyncResp, err
	}

	psyncResp, err = s.WaitForPsyncResp()
	if err != nil {
		return psyncResp, err
	}

	return psyncResp, nil
}

func (s *Server) WaitForPsyncResp() (PsyncResponse, error) {
	buf := make([]byte, 4096)
	var psyncResp PsyncResponse

	ctx, cancel := context.WithTimeout(context.TODO(), time.Duration(10)*time.Second) //give the master server 10 seconds to respond to PSYNC
	defer cancel()

	respChan := make(chan ([]byte))

	go func() {
		n, err := s.MasterConn.Read(buf)
		if err != nil {
			slog.Error("reading from master server connection", "err", err)
			respChan <- nil
		} else {
			got := buf[:n]
			respChan <- got
		}
	}()

	select {
	case got := <-respChan:
		cmd, _, ok := s.Parser.TryParsingCommand(got)
		if !ok {
			return psyncResp, errors.New("parsing response from master server after sending PSYNC")
		}

		var err error
		psyncResp, err = s.CommandToPsyncResp(cmd)
		if err != nil {
			return psyncResp, err
		}
	case <-ctx.Done():
		return psyncResp, errors.New("TIMEOUT replica server did not recieve PSYNC response in time")
	}

	return psyncResp, nil
}

func (s *Server) SendReplConf() error {
	bytes := s.Handler.Encoder.GenerateReplicaConfig(s.LocalPort)
	_, err := s.MasterConn.Write(bytes)
	if err != nil {
		return err
	}

	buf := make([]byte, 4096)
	n, err := s.MasterConn.Read(buf)
	if err != nil {
		return err
	}
	got := buf[:n]
	if eq := slices.Equal(got, s.Handler.Encoder.GetSimpleStringOk()); !eq {
		return errors.New("did not recieve ok response from master after sending REPLCONF")
	}

	return nil
}

func (s *Server) PingMasterConn() error {
	bytes := s.Handler.Encoder.GeneratePing()

	_, err := s.MasterConn.Write(bytes)
	if err != nil {
		return err
	}

	ok := s.WaitForPong()
	if !ok {
		return errors.New("failed handshake with master server: PING was not recieved")
	}

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

func (s *Server) IncrementReplicaOffset(cmd Command) {
	writeCommands := []string{"SET", "LPUSH", "RPUSH", "LPOP", "BRPOP", "INCR", "MULTI", "EXEC", "DISCARD"}

	if slices.Contains(writeCommands, cmd.Name) {
		s.ReplicationOffset += uint64(cmd.NumBytes)
	}
}

func (s *Server) ParseReplicaConfig(kvPair []string) {
	switch kvPair[0] {
	case "listening-port":
		s.replPortSet[kvPair[1]] = true
		slog.Info("registered a new replica port", "port", kvPair[1])
	}
}

func (s *Server) CommandToPsyncResp(cmd Command) (PsyncResponse, error) {
	if cmd.Name == "+CONTINUE" {
		return PsyncResponse{isPartialResync: true}, nil
	} else if cmd.Name == "+FULLRESYNC" {
		masterOffset, _ := strconv.Atoi(string(cmd.Args[1]))
		return PsyncResponse{isPartialResync: false, masterID: string(cmd.Args[0]), masterOffset: uint64(masterOffset)}, nil
	} else {
		return PsyncResponse{}, errors.New("recieved an invalid psync response from master server")
	}
}
