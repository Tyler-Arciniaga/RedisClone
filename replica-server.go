package main

import (
	"errors"
	"fmt"
	"log/slog"
	"net"
	"slices"
	"strconv"
)

// all logic pertaining to a replica server (i.e. not master)

func (s *Server) HandleReplicaStatus(repStatus ReplicaRequest) {
	if repStatus.isNowMaster {
		s.ConfigureMasterStatus()
	} else {
		port := fmt.Sprintf("localhost:%s", repStatus.masterPort)
		conn, err := net.Dial("tcp", port)
		s.MasterConn = conn
		if err != nil {
			s.MasterConn = nil //reset local server's master conn if TCP connection failed
		}

		psyncResp, err := s.EstablishMasterHandshake()
		if err != nil {
			slog.Error("Handshake with master server failed", "err", err.Error())
			return
		}

		if psyncResp.isPartialResync {
			slog.Info("Awaiting partial resync with master server...")
		} else {
			slog.Info("Awaiting full sync with master server", "masterReplID", psyncResp.masterID, "masterReplOffset", psyncResp.masterOffset)

			rdb := s.WaitForBytes(s.MasterConn, 30)
			if rdb == nil {
				slog.Error("recieving RDB snapshot from master server")
				return
			}

			s.LoadRDB(rdb)

			bytes := s.Handler.Encoder.GetSimpleStringOk()
			conn.Write(bytes)

			var commandBytes []byte
			commandBytes = s.WaitForBytes(s.MasterConn, 2)
			if commandBytes == nil {
				commandBytes = []byte{}
			}

			s.ApplyBufferedCommandBytes(commandBytes)
			conn.Write(s.Handler.Encoder.GetSimpleStringOk())

			slog.Info("Connection with master server established!")

			go s.HandleMasterServerStream(conn)
		}
	}
}

func (s *Server) HandleMasterServerStream(conn net.Conn) {
	buf := make([]byte, 4096)
	temp := make([]byte, 4096)

	for {
		n, err := conn.Read(temp)
		if err != nil {
			slog.Error("error reading an incoming command stream from master server")
			return
		}

		buf = append(buf, temp[:n]...)
		cmd, consumed, ok := s.Parser.TryParsingCommand(buf)
		if !ok {
			continue
		}

		if s.IsWriteCommand(cmd.Name) {
			//increment replica offset
			offsetChange := uint64(consumed)
			s.ReplicationOffset += offsetChange
		}

		buf = buf[consumed:]

		isAtomic := false
		fmt.Println(cmd)
		resp := s.HandleParsedCommands(cmd, isAtomic, conn)

		conn.Write(resp)
	}
}

func (s *Server) ApplyBufferedCommandBytes(commandBytes []byte) {
	for len(commandBytes) > 0 {
		cmd, consumed, ok := s.Parser.TryParsingCommand(commandBytes)
		if !ok {
			continue
		}

		commandBytes = commandBytes[consumed:]

		isAtomic := false
		var garbage net.Conn
		s.HandleParsedCommands(cmd, isAtomic, garbage)
	}

	slog.Info("Finished applying all buffered commands from master server")
}

func (s *Server) LoadRDB(rdb []byte) {
	//TODO parse RDB and load it into memory
	slog.Info("Finished loading RDB snapshot into memory")
}

// function executed by replica
func (s *Server) EstablishMasterHandshake() (PsyncResponse, error) {
	var psyncResp PsyncResponse

	err := s.PingMaster()
	if err != nil {
		return psyncResp, err
	}

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

// function executed by replica
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

// function executed by replica
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

// function executed by replica
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

// function executed by replica
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
