package main

import (
	"context"
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

// all functions pertaining to generic / master server

type Server struct {
	LocalPort         string
	ReplicationID     string
	ReplicationOffset uint64
	Role              string
	MasterPort        string //may be uninitialized if role is master
	MasterConn        net.Conn

	commandBuffer  []byte //stores commands while RDB snapshot is being created
	commandBacklog CommandBacklog

	clientConnSet *SafeMap[net.Conn, bool]
	replPortMap   *SafeMap[string, net.Conn]
	replicaSet    *SafeMap[net.Conn, bool]
	inProgReplMap *SafeMap[net.Conn, chan ([]byte)]
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
			fmt.Print("This is a multi-threaded Redis clone made entirely in Go!\n\nCommand Flags:\n--port [port number] : to configure the listening port\n--help : You're already here!\n")
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
		s.clientConnSet.InsertKV(c, true)
	}
}

func (s *Server) DisconnectConnections() {
	for c := range s.leaveChan {
		c.Close()

		if ok := s.clientConnSet.DeleteKey(c); ok {
			slog.Info("A client has disconnected")
		} else {
			s.replicaSet.DeleteKey(c)
			slog.Info("A replica has disconnected")
		}
	}
}

func (s *Server) HandleClientStream(conn net.Conn) {
	buf := make([]byte, 4096)
	temp := make([]byte, 4096)

	for {
		n, err := conn.Read(temp)
		if err != nil {
			s.leaveChan <- conn
			return
		}

		buf = append(buf, temp[:n]...)
		cmd, consumed, ok := s.Parser.TryParsingCommand(buf)
		if !ok {
			continue
		}

		if s.IsWriteCommand(cmd.Name) {
			//increment replica offset
			s.ReplicationOffset += uint64(consumed)

			//if there are any replicas waiting for an RDB snapshot add command to buffer
			if s.inProgReplMap.GetLen() > 0 {
				s.commandBuffer = append(s.commandBuffer, buf[:consumed]...)
			}

			s.commandBacklog.AddCommandBytes(buf[:consumed])

			go s.StreamCommandToReplicas(buf)
		}

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

		//TODO increment replication offset for transactions as well

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
	infoMap["connected_clients"] = s.clientConnSet.GetLen()
	infoMap["role"] = s.Role
	infoMap["master_replid"] = s.ReplicationID
	infoMap["master_repl_offset"] = s.ReplicationOffset
	return infoMap
}

func (s *Server) HandleParsedCommands(cmd Command, isAtomic bool, conn net.Conn) []byte {
	//the process of syncing with master server (partial or full sync) cannot hold rlock as then it can't execute initial buffered commands from master
	if !isAtomic && cmd.Name != "REPLICAOF" {
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

	return response
}

func (s *Server) IsWriteCommand(cmdName string) bool {
	writeCommands := []string{"SET", "LPUSH", "RPUSH", "LPOP", "BRPOP", "INCR", "MULTI", "EXEC", "DISCARD"}

	if slices.Contains(writeCommands, cmdName) {
		return true
	}

	return false
}

func (s *Server) StreamCommandToReplicas(commandBytes []byte) {
	replicaConns := s.replicaSet.GetKeys()
	for _, conn := range replicaConns {
		conn.Write(commandBytes)
	}
}

func (s *Server) HandleReplicaConfig(kvPair []string, conn net.Conn) {
	switch kvPair[0] {
	case "listening-port":
		s.replPortMap.InsertKV(kvPair[1], conn)
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
			// create send RDB file to replica
			replChan := make(chan ([]byte))
			var rdb []byte

			if s.inProgReplMap.GetLen() == 0 {
				//there are currently no replicas waiting for a RDB snapshot -> create a new background process to generate an RDB snapshot
				s.inProgReplMap.InsertKV(conn, replChan) //add current replica conn to set
				rdb = s.CreateRDB()
				go s.SendRDB(rdb)
				rdb = <-replChan
			} else {
				// there is at least one other replica waiting for an already in progress RDB snapshot
				s.inProgReplMap.InsertKV(conn, replChan) //add current replica conn to set
				rdb = <-replChan
			}

			conn.Write(rdb)

			bytes := s.WaitForBytes(conn, 5) //wait 5 seconds to recieve ok signal from replica server
			if bytes == nil {
				slog.Error("did not recieve ok response from replica after sending RDB snapshot")
				return
			}

			slog.Info("Recieved OK response from replicas after sending RDB snapshot!")

			if len(s.commandBuffer) > 0 {
				slog.Info("Sending buffered commands to new replicas...")
				s.SendCommandBufferToReplicas()

				bytes = s.WaitForBytes(conn, 10)
				if bytes == nil {
					return
				}

				slog.Info("Recieved OK response from replicas after sending buffered commands")
			}
		} else {

			// PARTIAL RESYNC **************************
			okSignal := s.WaitForBytes(conn, 10) //wait 10 seconds to recieve ok signal from replica about PSYNC response
			if okSignal == nil {
				slog.Error("did not recieve OK signal from replica server regarding PSYNC response")
				return
			}

			// stream the commands that the replica is missing and return
			replicaOffset, _ := strconv.Atoi(string(psyncReq.Args[1]))
			buf, ok := s.commandBacklog.ExtractNeededBytes(uint64(replicaOffset), s.ReplicationOffset)
			if !ok {
				//TODO return error and force replica into full sync (the replication backlog does not have all the bytes needed to get replica up to speed)
			}

			conn.Write(buf)

			okSignal = s.WaitForBytes(conn, 10) //recieve ok signal after replica confirms partial resync
			if okSignal == nil {
				slog.Error("did not recieve OK signal from replica server regarding partial resync")
				return
			}
			slog.Info("Finished partial resync with replica", "conn", conn)
		}
	}

	//at this point the connection is a fully established replica, thus we can begin streaming all our write commands to it
	//TODO: make the following thread safe
	s.clientConnSet.DeleteKey(conn)
	s.replicaSet.InsertKV(conn, true)
	slog.Info("New replica registered", "conn", conn)
}

func (s *Server) SendCommandBufferToReplicas() {
	replicaConns := s.inProgReplMap.GetKeys()
	for _, conn := range replicaConns {
		go func(commandBuffer []byte) {
			conn.Write(commandBuffer)
		}(s.commandBuffer)
	}

	s.commandBuffer = []byte{} //reset command buffer
}

func (s *Server) CreateRDB() []byte {
	var rdb []byte
	rdb = append(rdb, []byte("REDIS")...)
	rdb = append(rdb, []byte("0001")...)
	rdb = append(rdb, 0xFF)

	time.Sleep(2 * time.Second) // placeholder, this mimics the time it might take to generate a new RDB snapshot for the local data

	return rdb
}

func (s *Server) SendRDB(rdb []byte) {
	replyChans := s.inProgReplMap.GetValues()
	for _, replChan := range replyChans {
		replChan <- rdb
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
		} //error recieving bytes from destination connection

		return got

	case <-ctx.Done():
		slog.Error("TIMEOUT server did not recieve expected bytes in time")
		return nil
	}
}
