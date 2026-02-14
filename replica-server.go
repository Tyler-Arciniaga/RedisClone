package main

import (
	"errors"
	"fmt"
	"log/slog"
	"net"
	"slices"
	"strconv"
	"time"
)

// all logic pertaining to a replica server (not root master server)

func (s *Server) HandleReplicaStatus(replStatus ReplicaRequest) []byte {
	if replStatus.isNowMaster {
		s.ConfigureMasterStatus()
		slog.Info("Server is now a master")
	} else {
		err := s.HandleReplication(replStatus)
		if err != nil {
			return s.Handler.Encoder.GenerateSimpleError(err.Error())
		}
		slog.Info("Server is now a replica")
	}

	return s.Handler.Encoder.GetSimpleStringOk()
}

func (s *Server) HandleReplication(replStatus ReplicaRequest) error {
	port := fmt.Sprintf("localhost:%s", replStatus.masterPort)
	conn, err := net.Dial("tcp", port)
	if err != nil {
		s.MasterConn = nil //reset local server's master conn if TCP connection failed
		return errors.New("ERR could not establish TCP connection with master server")
	}
	s.MasterConn = conn // set master connection

	psyncResp, err := s.EstablishMasterHandshake()
	if err != nil {
		slog.Error("Handshake with master server failed", "err", err.Error())
		return errors.New("ERR Handshake with master server failed")
	}

	conn.Write(s.Handler.Encoder.GetSimpleStringOk()) //send ok signal to master server to signify that we recieved a PSYNC response (and begin sync between the two servers)

	//TODO check to see if master server has enough saved previous commands to partial resync with replica...

	//update replica's replicationID and offset to match the master server
	s.ReplicationID = psyncResp.masterID
	s.ReplicationOffset = psyncResp.masterOffset
	s.Role = "replica"

	if psyncResp.isPartialResync {
		s.PartialResyncWithMaster(psyncResp, conn)
	} else {
		err = s.FullSyncWithMaster(psyncResp, conn)
		if err != nil {
			return err
		}
	}

	slog.Info("Connection with master server established!")
	go s.HandleMasterServerStream(conn) //spin off a new go routine to handle all streamed write commands from master server
	s.StartSendingOffsetAcks()

	return nil
}

func (s *Server) PartialResyncWithMaster(psyncResp PsyncResponse, conn net.Conn) {
	slog.Info("Starting partial resync with master server...")

	commandBytes := s.WaitForBytes(conn, 10) //wait 10 seconds to recieve bytes need to bring replica up to date with master server's data
	if commandBytes == nil {
		commandBytes = []byte{}
	}

	s.ApplyCommandBytes(commandBytes)

	conn.Write(s.Handler.Encoder.GetSimpleStringOk())
}

func (s *Server) FullSyncWithMaster(psyncResp PsyncResponse, conn net.Conn) error {
	slog.Info("Awaiting full sync with master server", "masterReplID", psyncResp.masterID, "masterReplOffset", psyncResp.masterOffset)

	rdb := s.WaitForBytes(s.MasterConn, 30) // wait at most 30 seconds to recieve RDB from master server
	if rdb == nil {
		slog.Error("recieving RDB snapshot from master server")
		return errors.New("recieving RDB snapshot from master server")
	}

	err := s.LoadRDB(rdb)
	if err != nil {
		return err
	}

	conn.Write(s.Handler.Encoder.GetSimpleStringOk()) //send ok reply to master after loading RDB

	var commandBytes []byte
	commandBytes = s.WaitForBytes(s.MasterConn, 5) //wait to recieve buffered command bytes (if any)
	if commandBytes == nil {
		commandBytes = []byte{}
		slog.Info("No buffered command bytes recieved from master server")
	}

	s.ApplyCommandBytes(commandBytes)
	conn.Write(s.Handler.Encoder.GetSimpleStringOk())

	return nil
}

func (s *Server) HandleMasterServerStream(conn net.Conn) {
	buf := []byte{} // set buf intially to empty string rather than of size 4096 to fix bug with initial offset handling
	temp := make([]byte, 4096)

	for {
		n, err := conn.Read(temp)
		if err != nil {
			slog.Error("error reading an incoming command stream from master server, transition into master server...")
			s.Role = "master"
			return
		}

		buf = append(buf, temp[:n]...)
		cmd, consumed, ok := s.Parser.TryParsingCommand(buf)
		if !ok {
			continue
		}

		s.ReplicationOffset += uint64(consumed) //guranteed to be a write command therefore always increment replica offset

		buf = buf[consumed:]

		isAtomic := false
		s.HandleParsedCommands(cmd, isAtomic, conn)
	}
}

func (s *Server) StartSendingOffsetAcks() {
	ticker := time.NewTicker(time.Duration(2) * time.Second)
	stopChan := make(chan (bool))

	s.AckTicker = ticker
	s.AckStopChan = stopChan

	go s.SendOffsetAcks(s.MasterConn, ticker, stopChan) // spin of a new go routine to periodically send a heatbeat / offset ACK to master server
}

func (s *Server) SendOffsetAcks(conn net.Conn, ticker *time.Ticker, returnChan chan (bool)) {
	for {
		select {
		case <-ticker.C:
			conn.Write(s.Handler.Encoder.GenerateOffsetAck(s.ReplicationOffset))
		case <-returnChan:
			return
		}
	}
}

func (s *Server) ApplyCommandBytes(commandBytes []byte) {
	for len(commandBytes) > 0 {
		cmd, consumed, ok := s.Parser.TryParsingCommand(commandBytes)
		if !ok {
			slog.Error("Error parsing some of the command bytes that were streamed from master server")
			break
		}

		s.ReplicationOffset += uint64(consumed)

		commandBytes = commandBytes[consumed:]

		isAtomic := false
		var garbage net.Conn
		s.HandleParsedCommands(cmd, isAtomic, garbage)
	}

	slog.Info("Finished applying all buffered commands from master server")
}

func (s *Server) LoadRDB(rdb []byte) error {
	//TODO parse RDB and load it into memory
	//save to Disk
	//read from Disk
	slog.Info("Finished loading RDB snapshot into memory")

	return nil
}

func (s *Server) EstablishMasterHandshake() (PsyncResponse, error) {
	err := s.PingMaster()
	if err != nil {
		return PsyncResponse{}, err
	}

	err = s.SendReplConf()
	if err != nil {
		return PsyncResponse{}, err
	}

	psyncResp, err := s.ExchangePsync()
	if err != nil {
		return PsyncResponse{}, err
	}

	return psyncResp, nil
}

func (s *Server) PingMaster() error {
	bytes := s.Handler.Encoder.GeneratePing()

	_, err := s.MasterConn.Write(bytes)
	if err != nil {
		return err
	}

	bytes = s.WaitForBytes(s.MasterConn, 10)
	if bytes == nil {
		return errors.New("failed handshake with master server: PING was not recieved")
	}

	expect := s.Handler.Encoder.GenerateSimpleString([]byte("PONG"))
	if eq := slices.Equal(expect, bytes); !eq {
		return errors.New("ERROR recieved incorrect signal from master server after sending PING")
	}

	return nil
}

func (s *Server) SendReplConf() error {
	bytes := s.Handler.Encoder.GenerateReplicaConfig(s.LocalPort)
	_, err := s.MasterConn.Write(bytes)
	if err != nil {
		return err
	}

	bytes = s.WaitForBytes(s.MasterConn, 10)
	if bytes == nil {
		return errors.New("did not recieve ok response from master after sending REPLCONF")
	}

	if eq := slices.Equal(bytes, s.Handler.Encoder.GetSimpleStringOk()); !eq {
		return errors.New("did not recieve ok response from master after sending REPLCONF")
	}

	return nil
}

func (s *Server) ExchangePsync() (PsyncResponse, error) {
	//send PSYNC command
	var psyncResp PsyncResponse
	bytes := s.Handler.Encoder.GeneratePsync(s.ReplicationID, s.ReplicationOffset)
	_, err := s.MasterConn.Write(bytes)
	if err != nil {
		return psyncResp, err
	}

	bytes = s.WaitForBytes(s.MasterConn, 10)
	if bytes == nil {
		return psyncResp, errors.New("did not recieve response to PSYNC from master")
	}

	cmd, _, ok := s.Parser.TryParsingCommand(bytes)
	if !ok {
		return psyncResp, errors.New("parsing response from master server after sending PSYNC")
	}
	psyncResp, err = s.CommandToPsyncResp(cmd)
	if err != nil {
		return psyncResp, err
	}

	return psyncResp, nil
}

func (s *Server) CommandToPsyncResp(cmd Command) (PsyncResponse, error) {
	switch cmd.Name {
	case "+CONTINUE":
		return PsyncResponse{isPartialResync: true}, nil
	case "+FULLRESYNC":
		masterOffset, _ := strconv.Atoi(string(cmd.Args[1]))
		return PsyncResponse{isPartialResync: false, masterID: string(cmd.Args[0]), masterOffset: uint64(masterOffset)}, nil
	default:
		return PsyncResponse{}, errors.New("recieved an invalid psync response from master server")
	}
}
