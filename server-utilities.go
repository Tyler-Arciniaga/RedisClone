package main

import (
	"log/slog"
	"math/rand"
	"net"
	"slices"
	"strings"
	"time"
)

// collection of small, stateless, reusable master server utilities

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

	if s.MasterConn != nil {
		s.MasterConn.Close()
	}
	s.MasterConn = nil

	if s.AckTicker != nil {
		s.AckTicker.Stop()
		s.AckStopChan <- true
	}
	s.AckTicker = nil
	s.AckStopChan = nil
}

func (s *Server) WaitForBytes(conn net.Conn, duration uint64) []byte {
	conn.SetReadDeadline(time.Now().Add(time.Duration(duration) * time.Second))
	defer conn.SetReadDeadline(time.Time{}) //remove read deadline on conn

	buf := make([]byte, 4096)
	n, err := conn.Read(buf)
	if err != nil {
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			slog.Error("TIMEOUT server did not recieve expected bytes in time")
		} else {
			slog.Error("reading from connection", "err", err)
		}

		return nil
	}

	return buf[:n]
}

func (s *Server) IsWriteCommand(cmdName string) bool {
	writeCommands := []string{"SET", "LPUSH", "RPUSH", "LPOP", "BRPOP", "INCR", "MULTI", "EXEC", "DISCARD", "WAIT"}
	if slices.Contains(writeCommands, cmdName) {
		return true
	}
	return false
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
