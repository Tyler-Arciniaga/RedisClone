package main

import (
	"fmt"
	"log/slog"
	"net"
	"os"
	"strconv"
	"sync"
	"time"
)

//TODO restrict commands for clients in subscribed mode -> track client state across client set

// all functions pertaining to generic / master server

type Server struct {
	LocalPort         string
	ReplicationID     string
	ReplicationOffset uint64
	Role              string
	MasterPort        string //may be uninitialized if role is master
	MasterConn        net.Conn

	AckTicker   *time.Ticker
	AckStopChan chan (bool)

	commandBuffer  []byte //stores commands while RDB snapshot is being created
	commandBacklog CommandBacklog

	clientConnSet    *SafeMap[net.Conn, *ClientObject]
	replPortMap      *SafeMap[string, net.Conn]
	replicaOffsetMap *SafeMap[net.Conn, uint64] //maps replicas to their offset
	inProgReplSet    *SafeMap[net.Conn, bool]

	subscribeChannels *SafeMap[net.Conn, bool]

	joinChan    chan (*ClientObject)
	leaveChan   chan (Node)
	HandlerLock sync.RWMutex

	Parser  *Parser
	Handler *Handler
}

// Bind to port, start new tcp server, and listen for client connections
func (s *Server) StartServer() {
	s.ConfigureMasterStatus() //default to master status

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

		c := &ClientObject{Conn: conn, NumSubscribedChannels: 0}
		s.joinChan <- c
		go s.HandleClientStream(c)
	}
}

func (s *Server) RegisterNewConnections() {
	for c := range s.joinChan {
		s.clientConnSet.UpsertKV(c.Conn, c)
	}
}

func (s *Server) DisconnectConnections() {
	for node := range s.leaveChan {
		conn := node.GetConn()
		conn.Close()

		if ok := s.clientConnSet.DeleteKey(conn); ok {
			slog.Info("A client has disconnected")
		} else {
			s.replicaOffsetMap.DeleteKey(conn)
			slog.Info("A replica has disconnected")
		}
	}
}

func (s *Server) HandleClientStream(c *ClientObject) {
	buf := make([]byte, 4096)
	temp := make([]byte, 4096)

	var singleCommand []byte
	var resp []byte
	for {
		n, err := c.Conn.Read(temp)
		if err != nil {
			s.leaveChan <- c
			return
		}

		buf = append(buf, temp[:n]...)
		cmd, consumed, ok := s.Parser.TryParsingCommand(buf)
		if !ok {
			continue
		}

		singleCommand = buf

		buf = buf[consumed:]

		isAtomic := false

		if c.NumSubscribedChannels > 0 {
			resp = s.HandleSubscribedClientCommands(cmd, c.Conn)
		} else {
			resp = s.HandleParsedCommands(cmd, isAtomic, c.Conn)
		}

		if s.IsWriteCommand(cmd.Name) {
			//increment replica offset
			s.ReplicationOffset += uint64(consumed)

			//if there are any replicas waiting for an RDB snapshot add command to buffer
			if s.inProgReplSet.GetLen() > 0 {
				s.commandBuffer = append(s.commandBuffer, singleCommand...)
			}

			s.commandBacklog.AddCommandBytes(singleCommand)

			go s.StreamCommandToReplicas(singleCommand)

			if cmd.Name == "WAIT" {
				//handle WAIT command logic
				resp = s.WaitForReplicas(cmd)
			}
		}

		c.Conn.Write(resp)

		if cmd.Name == "MULTI" {
			//enter transaction mode for this client
			s.HandleClientTransaction(c)
		}

	}
}

func (s *Server) WaitForReplicas(cmd Command) []byte {
	if s.replicaOffsetMap.GetLen() == 0 {
		return s.Handler.Encoder.GenerateInt(0)
	}

	localOffset := s.ReplicationOffset
	numReplicas, _ := strconv.Atoi(string(cmd.Args[0]))
	timeout, _ := strconv.Atoi(string(cmd.Args[1]))
	confirmedReplicaSet := NewSafeMap[net.Conn, bool]()

	var timer *time.Timer

	if timeout != 0 {
		timer = time.NewTimer(time.Duration(timeout) * time.Millisecond)
		defer timer.Stop()
	}

	for {
		numConfirmed := confirmedReplicaSet.GetLen()
		if numConfirmed >= numReplicas {
			return s.Handler.Encoder.GenerateInt(numConfirmed)
		}

		if timeout != 0 {
			select {
			case <-timer.C:
				return s.Handler.Encoder.GenerateInt(0)
			default:
				break
			}
		}

		for _, item := range s.replicaOffsetMap.GetItems() {
			go func() {
				conn := item[0].(net.Conn)
				offset := item[1].(uint64)
				// fmt.Println(conn, offset)
				if offset >= localOffset {
					// fmt.Println("here")
					confirmedReplicaSet.UpsertKV(conn, true)
				}
			}()
		}

	}
}

func (s *Server) HandleClientTransaction(c *ClientObject) {
	buf := make([]byte, 4096)
	temp := make([]byte, 4096)

	for {
		n, err := c.Conn.Read(temp)
		if err != nil {
			slog.Error(err.Error())
			s.leaveChan <- c
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
			s.ExecuteTransaction(c.Conn)
			return
		case "DISCARD":
			resp := s.Handler.DiscardCommandQueue(c.Conn)
			c.Conn.Write(resp)
			return
		case "MULTI":
			resp := s.Handler.Encoder.GenerateSimpleError("ERR cannot nest MULTI commands")
			c.Conn.Write(resp)
		default:
			resp := s.Handler.QueueCommand(cmd, c.Conn)
			c.Conn.Write(resp)
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

func (s *Server) HandleSubscribedClientCommands(cmd Command, conn net.Conn) []byte {
	var response []byte

	switch cmd.Name {
	case "SUBSCRIBE":
		numChans := s.Handler.HandleSubscribeCommand(cmd, conn)
		client, _ := s.clientConnSet.GetValue(conn)
		client.NumSubscribedChannels = numChans
	case "PING":
		response = s.Handler.HandleSubscribedPingCommand(cmd)
	case "UNSUBSCRIBE":
		numChans := s.Handler.HandleUnsubscribeCommand(cmd, conn)
		client, _ := s.clientConnSet.GetValue(conn)
		client.NumSubscribedChannels = numChans
	default:
		response = s.Handler.Encoder.GenerateSimpleError(fmt.Sprintf("ERR unknown command '%s' while in subscribed mode", cmd.Name))
	}

	return response
}

func (s *Server) HandleParsedCommands(cmd Command, isAtomic bool, conn net.Conn) []byte {
	//the process of syncing with master server (partial or full sync) cannot hold rlock because then it can't execute initial buffered commands from master
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
		repStatus := s.Handler.HandleReplicaOfCommand(cmd)
		response = s.HandleReplicaStatus(repStatus)
	case "REPLCONF":
		kvPair := s.Parser.ParseReplConfig(cmd)
		s.HandleReplicaConfig(kvPair, conn) //internal Redis command
	case "SUBSCRIBE":
		numChans := s.Handler.HandleSubscribeCommand(cmd, conn)
		client, _ := s.clientConnSet.GetValue(conn)
		client.NumSubscribedChannels = numChans
	case "PUBLISH":
		response = s.Handler.HandlePublishCommand(cmd)

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

func (s *Server) StreamCommandToReplicas(commandBytes []byte) {
	replicaConns := s.replicaOffsetMap.GetKeys()
	for _, conn := range replicaConns {
		go func() {
			conn.Write(commandBytes)
		}()
	}
}

func (s *Server) SendRDB(rdb []byte) {
	replicaConns := s.inProgReplSet.GetKeys()
	for _, conn := range replicaConns {
		go func() {
			conn.Write(rdb)
		}() //write rdb in a go func in order to not be blocked by slow replicas
	}
}

func (s *Server) SendCommandBufferToReplicas() {
	replicaConns := s.inProgReplSet.GetKeys()
	for _, conn := range replicaConns {
		go func(commandBuffer []byte) {
			conn.Write(commandBuffer)
		}(s.commandBuffer)
	}
	s.commandBuffer = []byte{} //clear command buffer
}

func (s *Server) EstablishReplica(conn net.Conn) bool {
	//wait for PSYNC req from replica
	bytes := s.WaitForBytes(conn, 10) //wait 10 seconds to recieve PSYNC req
	if bytes == nil {
		return false
	}

	//Parse PSYNC request
	psyncReq, _, ok := s.Parser.TryParsingCommand(bytes)
	if !ok {
		slog.Error("recieved invalid Psync request from replicating server")
		return false
	}

	//write PSYNC response to replica
	psyncResp, needsFullSync := s.Handler.HandlePsyncCommand(psyncReq, s.ReplicationID, s.ReplicationOffset, &s.commandBacklog)

	conn.Write(psyncResp)

	okSignal := s.WaitForBytes(conn, 10) //wait to recieve ok signal from replica about PSYNC response
	if okSignal == nil {
		slog.Error("did not recieve OK signal from replica server regarding PSYNC response")
		return false
	}

	if needsFullSync {
		ok := s.HandleFullSync(conn)
		if !ok {
			return false
		}
	} else {
		ok := s.HandlePartialResync(conn, psyncReq)
		if !ok {
			return false
		}
	}

	return true
}

func (s *Server) HandleFullSync(conn net.Conn) bool {
	// create and send RDB file to all waiting replicas
	var rdb []byte

	//if this is the first time a full sync is being requested...
	if s.inProgReplSet.GetLen() == 0 {
		defer s.inProgReplSet.Clear() // always clear the in progress replica set when the first replica queued to recieve RDB finishes (they are the lead replica)

		//there are currently no replicas waiting for a RDB snapshot -> create a new background process to generate an RDB snapshot
		s.inProgReplSet.UpsertKV(conn, true) //add current replica conn to set

		rdb = s.CreateRDB()
		s.SendRDB(rdb)

		ok := s.WaitForBytes(conn, 10) //wait for ok signal from replica regarding RDB file
		if ok == nil {
			return false
		}

		s.SendCommandBufferToReplicas()

		ok = s.WaitForBytes(conn, 30) //wait for ok signal from replica regarding command buffer
		if ok == nil {
			return false
		}

	} else {
		// there is at least one other replica waiting for an already in progress RDB snapshot...
		s.inProgReplSet.UpsertKV(conn, true) //add current replica conn to set

		ok := s.WaitForBytes(conn, 10) //wait for ok signal from replica regarding RDB file
		if ok == nil {
			return false
		}
		ok = s.WaitForBytes(conn, 30) //wait for ok signal from replica regarding command buffer
		if ok == nil {
			return false
		}
	}

	slog.Info("Finished full sync with a replica")

	return true
}

func (s *Server) HandlePartialResync(conn net.Conn, psyncReq Command) bool {
	// stream the commands that the replica is missing and return
	replicaOffset, _ := strconv.Atoi(string(psyncReq.Args[1]))
	buf, ok := s.commandBacklog.ExtractNeededBytes(uint64(replicaOffset), s.ReplicationOffset)
	if !ok {
		//TODO return error and force replica into full sync (the replication backlog does not have all the bytes needed to get replica up to speed)
		slog.Info("Not enough command bytes in backlog for partial resync, falling back to full sync...")
		ok = s.HandleFullSync(conn)
		if !ok {
			return false
		} // if full sync was unsuccessfull...
	}

	conn.Write(buf)

	okSignal := s.WaitForBytes(conn, 10) //wait to recieve ok signal after replica confirms partial resync
	if okSignal == nil {
		slog.Error("did not recieve OK signal from replica server regarding partial resync")
		return false
	}

	slog.Info("Finished partial resync with replica")
	return true
}

func (s *Server) HandleReplicaConfig(kvPair []string, conn net.Conn) {
	switch kvPair[0] {
	case "listening-port":
		s.replPortMap.UpsertKV(kvPair[1], conn)
		slog.Info("registered a new replica port", "port", kvPair[1])
		conn.Write(s.Handler.Encoder.GetSimpleStringOk()) //reply to the replica's REPLCONF msg

		ok := s.EstablishReplica(conn)
		if ok {
			//at this point the connection is a fully established replica, thus we can begin streaming all our write commands to it
			s.replicaOffsetMap.UpsertKV(conn, s.ReplicationOffset)
			s.clientConnSet.DeleteKey(conn)
			slog.Info("New replica registered!", "conn", conn)
			// go s.AcceptOffsetAcks(conn)
		}
	case "ACK":
		replicaOffset, _ := strconv.Atoi(string(kvPair[1]))
		s.replicaOffsetMap.UpsertKV(conn, uint64(replicaOffset))
	}
}

func (s *Server) CreateRDB() []byte {
	var rdb []byte
	rdb = append(rdb, []byte("REDIS")...)
	rdb = append(rdb, []byte("0001")...)
	rdb = append(rdb, 0xFF)

	time.Sleep(1 * time.Second) // placeholder, this mimics the time it might take to generate a new RDB snapshot for the local data

	return rdb
}
