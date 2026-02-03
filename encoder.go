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
		data = append(data, e.GetServerInfo(req.ServerInfo.ServerInfoMap)...)
	}
	if req.hasClient {
		data = append(data, e.GetClientInfo(req.ServerInfo.ClientInfoMap)...)
	}
	if req.hasReplication {
		data = append(data, e.GetReplicationInfo(req.ServerInfo.ReplicationInfoMap)...)
	}

	return e.GenerateBulkString(data)
}

func (e *Encoder) GetServerInfo(m map[string]any) []byte {
	out := make([]byte, 0)
	out = append(out, []byte("# Server\n")...)
	out = append(out, []byte("redis_clone_version:1.0\n")...)
	out = append(out, []byte(fmt.Sprint("os:", runtime.GOOS, "\n"))...)
	out = append(out, []byte("tcp_port:6379\n")...)
	out = append(out, '\n')

	return out
}

func (e *Encoder) GetClientInfo(m map[string]any) []byte {
	out := make([]byte, 0)
	out = append(out, []byte("# Client\n")...)
	out = append(out, []byte(fmt.Sprint("connected_clients:", m["num-clients"], "\n"))...)
	out = append(out, []byte(fmt.Sprint("blocked_clients:", m["num-blocked-clients"], "\n"))...)
	out = append(out, '\n')

	return out
}

func (e *Encoder) GetReplicationInfo(m map[string]any) []byte {
	out := make([]byte, 0)
	out = append(out, []byte("# Replication\n")...)
	out = append(out, []byte("role:master\n")...)
	out = append(out, '\n')

	return out
}
