package main

import (
	"fmt"
	"runtime"
	"strconv"
)

type Encoder struct {
	EncodingMap map[string][]byte
}

func (e *Encoder) InitalizeEncodingMap() {
	e.EncodingMap = make(map[string][]byte)
	e.EncodingMap["OK"] = e.GenerateSimpleString([]byte("OK"))
	e.EncodingMap["nil"] = e.GenerateNilBulkString()
	e.EncodingMap["Queued"] = e.GenerateSimpleString([]byte("Queued"))
}

func (e *Encoder) GetSimpleStringOk() []byte {
	return e.EncodingMap["OK"]
}

func (e *Encoder) GetNilBulkString() []byte {
	return e.EncodingMap["nil"]
}

func (e *Encoder) GetSimpleStringQueued() []byte {
	return e.EncodingMap["Queued"]
}

func (e *Encoder) GenerateTypeString(t NativeType) []byte {
	var response []byte
	switch t {
	case Bytes:
		response = e.GenerateSimpleString([]byte("string"))
	case List:
		response = e.GenerateSimpleString([]byte("list"))
	case Stream:
		response = e.GenerateSimpleString([]byte("stream"))
	case None:
		response = e.GenerateSimpleString([]byte("none"))
	}

	return response
}

func (e *Encoder) GenerateBulkString(bytes []byte) []byte {
	out := make([]byte, 0, len(bytes)+32)
	out = append(out, '$')
	out = strconv.AppendInt(out, int64(len(bytes)), 10)
	out = append(out, '\r', '\n')
	out = append(out, bytes...)
	out = append(out, '\r', '\n')
	return out
}

func (e *Encoder) GenerateInt(i int) []byte {
	out := make([]byte, 0, 32)
	out = append(out, ':')
	out = strconv.AppendInt(out, int64(i), 10)
	out = append(out, '\r', '\n')
	return out
}

func (e *Encoder) GenerateArray(array [][]byte, isForTransaction bool) []byte {
	out := make([]byte, 0, len(array)+32)
	out = append(out, '*')
	out = strconv.AppendInt(out, int64(len(array)), 10)
	out = append(out, '\r', '\n')
	for _, v := range array {
		if isForTransaction {
			out = append(out, v...)
		} else {
			out = append(out, e.GenerateBulkString(v)...)
		}
	}

	return out
}

func (e *Encoder) GenerateNilArray() []byte {
	out := make([]byte, 0)
	out = append(out, '*')
	out = strconv.AppendInt(out, int64(-1), 10)
	out = append(out, '\r', '\n')
	return out
}

func (e *Encoder) GenerateNilBulkString() []byte {
	out := make([]byte, 0)
	out = append(out, '$')
	out = strconv.AppendInt(out, -1, 10)
	out = append(out, '\r', '\n')
	return out
}
func (e *Encoder) GenerateSimpleString(bytes []byte) []byte {
	out := make([]byte, 0, len(bytes)+32)
	out = append(out, '+')
	out = append(out, bytes...)
	out = append(out, '\r', '\n')
	return out
}

func (e *Encoder) GenerateSimpleError(err string) []byte {
	bytes := []byte(err)
	out := make([]byte, 0, len(bytes)+32)
	out = append(out, '-')
	out = append(out, bytes...)
	out = append(out, '\r', '\n')
	return out
}

func (e *Encoder) GetSysInfo(req InfoRequest) []byte {
	var data []byte
	if req.hasServer {
		data = append(data, e.GetServerInfo(req.serverInfo)...)
	}
	if req.hasClient {
		data = append(data, e.GetClientInfo(req.serverInfo)...)
	}
	if req.hasReplication {
		data = append(data, e.GetReplicationInfo(req.serverInfo)...)
	}

	return e.GenerateBulkString(data)
}

func (e *Encoder) GetServerInfo(m map[string]any) []byte {
	out := make([]byte, 0)
	out = append(out, []byte("# Server\n")...)
	out = append(out, []byte("redis__custom_clone_version:1.0\n")...)
	out = append(out, []byte(fmt.Sprint("os:", runtime.GOOS, "\n"))...)
	out = append(out, []byte(fmt.Sprint("tcp_port:", m["tcp_port"], "\n"))...)
	out = append(out, '\n')

	return out
}

func (e *Encoder) GetClientInfo(m map[string]any) []byte {
	out := make([]byte, 0)
	out = append(out, []byte("# Client\n")...)
	out = append(out, []byte(fmt.Sprint("connected_clients:", m["connected_clients"], "\n"))...)
	out = append(out, []byte(fmt.Sprint("blocked_clients:", m["blocked_clients"], "\n"))...)
	out = append(out, '\n')

	return out
}

func (e *Encoder) GetReplicationInfo(m map[string]any) []byte {
	out := make([]byte, 0)
	out = append(out, []byte("# Replication\n")...)
	out = append(out, []byte(fmt.Sprint("role:", m["role"], "\n"))...)
	out = append(out, []byte(fmt.Sprint("master_replid:", m["master_replid"], "\n"))...)
	out = append(out, []byte(fmt.Sprint("master_repl_offset:", m["master_repl_offset"], "\n"))...)
	out = append(out, '\n')

	return out
}

func (e *Encoder) GeneratePing() []byte {
	command := [][]byte{[]byte("PING")}
	isForTransaction := false
	return e.GenerateArray(command, isForTransaction)
}

func (e *Encoder) GenerateReplicaConfig(localPort string) []byte {
	command := [][]byte{[]byte("REPLCONF"), []byte("listening-port"), []byte(localPort)}
	isForTransaction := false
	return e.GenerateArray(command, isForTransaction)
}

func (e *Encoder) GenerateOffsetAck(replOffset uint64) []byte {

	command := [][]byte{[]byte("REPLCONF"), []byte("ACK"), []byte(fmt.Sprintf("%d", replOffset))}
	isForTransaction := false
	return e.GenerateArray(command, isForTransaction)
}

func (e *Encoder) GeneratePsync(replID string, replOffset uint64) []byte {
	command := [][]byte{[]byte("PSYNC"), []byte(replID), []byte(fmt.Sprintf("%d", replOffset))}
	isForTransaction := false
	return e.GenerateArray(command, isForTransaction)
}

func (e *Encoder) GenerateFullResyncResp(localReplID string, localReplOffset uint64) []byte {
	command := [][]byte{[]byte("+FULLRESYNC"), []byte(localReplID), []byte(fmt.Sprintf("%d", localReplOffset))}
	isForTransaction := false
	return e.GenerateArray(command, isForTransaction)
}

func (e *Encoder) GeneratePartialResyncResp() []byte {
	command := [][]byte{[]byte("+CONTINUE")}
	isForTransaction := false
	return e.GenerateArray(command, isForTransaction)
}
