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
		}
	}
}

// function executed by replica
func (s *Server) EstablishMasterHandshake() (PsyncResponse, error) {
	var psyncResp PsyncResponse

	err := s.PingMaster()
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

// function executed by replica
func (s *Server) ExchangePsync() (PsyncResponse, error) {
	//send PSYNC command
	var psyncResp PsyncResponse
	bytes := s.Handler.Encoder.GeneratePsync(s.ReplicationID, s.ReplicationOffset)
	_, err := s.MasterConn.Write(bytes)
	if err != nil {
		return psyncResp, err
	}

	bytes = s.WaitForBytes(s.MasterConn)
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

// // function executed by replica
// func (s *Server) WaitForPsyncResp() (PsyncResponse, error) {
// 	buf := make([]byte, 4096)
// 	var psyncResp PsyncResponse

// 	ctx, cancel := context.WithTimeout(context.TODO(), time.Duration(10)*time.Second) //give the master server 10 seconds to respond to PSYNC
// 	defer cancel()

// 	respChan := make(chan ([]byte))

// 	go func() {
// 		n, err := s.MasterConn.Read(buf)
// 		if err != nil {
// 			slog.Error("reading from master server connection", "err", err)
// 			respChan <- nil
// 		} else {
// 			got := buf[:n]
// 			respChan <- got
// 		}
// 	}()

// 	select {
// 	case got := <-respChan:

// 	case <-ctx.Done():
// 		return psyncResp, errors.New("TIMEOUT replica server did not recieve PSYNC response in time")
// 	}

// 	return psyncResp, nil
// }

// function executed by replica
func (s *Server) SendReplConf() error {
	bytes := s.Handler.Encoder.GenerateReplicaConfig(s.LocalPort)
	_, err := s.MasterConn.Write(bytes)
	if err != nil {
		return err
	}

	bytes = s.WaitForBytes(s.MasterConn)
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

	bytes = s.WaitForBytes(s.MasterConn)
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
