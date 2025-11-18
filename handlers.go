package main

import (
	"strconv"
	"time"
)

type Handler struct {
	Store *Store
}

func (h Handler) HandlePingCommand(cmd Command) []byte {
	if len(cmd.Args) == 0 {
		return h.GenerateSimpleString([]byte("PONG"))
	}
	return h.GenerateBulkString(cmd.Args[0])

}

func (h Handler) HandleEchoCommand(cmd Command) []byte {
	return h.GenerateBulkString(cmd.Args[0])
}

func (h Handler) HandleSetCommand(cmd Command) []byte {
	var d StoreData
	d.data = cmd.Args[1]
	for i := 0; i < len(cmd.Args); i++ {
		if string(cmd.Args[i]) == "EX" {
			ttl, _ := strconv.Atoi(string(cmd.Args[i+1])) //TODO handle potential error
			d.ttl = (time.Now().Add(time.Duration(ttl) * time.Second))
		}
	}
	h.Store.SetKeyVal(cmd.Args[0], d)
	return h.GenerateSimpleString([]byte("OK"))
}

func (h Handler) HandleGetCommand(cmd Command) []byte {
	v := h.Store.GetKeyVal(cmd.Args[0])
	if v == nil {
		return h.GenerateNilBulkString()
	}
	return h.GenerateBulkString(v)

}

func (h Handler) GenerateBulkString(bytes []byte) []byte {
	out := make([]byte, 0, len(bytes)+32)
	out = append(out, '$')
	out = strconv.AppendInt(out, int64(len(bytes)), 10)
	out = append(out, '\r', '\n')
	out = append(out, bytes...)
	out = append(out, '\r', '\n')
	return out
}

func (h Handler) GenerateNilBulkString() []byte {
	out := make([]byte, 0)
	out = append(out, '$')
	out = strconv.AppendInt(out, -1, 10)
	out = append(out, '\r', '\n')
	return out
}
func (h Handler) GenerateSimpleString(bytes []byte) []byte {
	out := make([]byte, 0, len(bytes)+32)
	out = append(out, '+')
	out = append(out, bytes...)
	out = append(out, '\r', '\n')
	return out
}
