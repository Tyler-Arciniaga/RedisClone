package main

import (
	"bytes"
	"log/slog"
	"strconv"
)

//TODO add error handling to all handler functions

type Handler struct {
	Store   *Store
	Encoder Encoder
}

type Option struct {
	Name string
	Arg  any
}

type SetRequest struct {
	Key     string
	Value   []byte
	Options []Option
}

func (h *Handler) InitalizeHandler() {
	h.Encoder.InitalizeEncodingMap()
}

func (h *Handler) HandlePingCommand(cmd Command) []byte {
	if len(cmd.Args) == 0 {
		return h.Encoder.GenerateSimpleString([]byte("PONG"))
	}
	return h.Encoder.GenerateBulkString(cmd.Args[0])

}

func (h *Handler) HandleEchoCommand(cmd Command) []byte {
	return h.Encoder.GenerateBulkString(cmd.Args[0])
}

func (h *Handler) HandleSetCommand(cmd Command) []byte {
	options := h.ParseOptions(cmd)
	sr := SetRequest{Key: string(cmd.Args[0]), Value: cmd.Args[1], Options: options}
	h.Store.SetKeyVal(sr)

	return h.Encoder.GetSimpleStringOk()
}

// TODO fix me!!!
func (h *Handler) ParseOptions(cmd Command) []Option {
	var options []Option
	switch cmd.Name {
	case "SET":
		exOption := []byte("EX")
		optionPortion := cmd.Args[2:]
		for i := 0; i < len(optionPortion); i++ {
			if bytes.Equal(optionPortion[i], exOption) {
				ttl, _ := strconv.Atoi(string(optionPortion[i+1])) //TODO handle potential error
				o := Option{Name: "EX", Arg: ttl}
				options = append(options, o)
			}
		}
	}
	return options
}

func (h *Handler) HandleGetCommand(cmd Command) []byte {
	key := string(cmd.Args[0])
	v := h.Store.GetKeyVal(key)
	if v == nil {
		return h.Encoder.GetNilBulkString()
	}
	return h.Encoder.GenerateBulkString(v)

}

// List Commands
func (h *Handler) HandleListPushCommand(cmd Command) []byte {
	var lc ListModificationRequest
	lc.Name = cmd.Name
	lc.Key = string(cmd.Args[0])
	lc.Values = append(lc.Values, cmd.Args[1:]...)

	listLength := h.Store.ListPush(lc)
	resp := h.Encoder.GenerateInt(listLength)

	return resp
}

func (h *Handler) HandleListRangeCommand(cmd Command) []byte {
	var lc ListRangeRequest
	lc.Name = cmd.Name
	lc.Key = string(cmd.Args[0])
	start, err1 := strconv.Atoi(string(cmd.Args[1]))
	end, err2 := strconv.Atoi(string(cmd.Args[2]))
	if err1 != nil || err2 != nil {
		slog.Error("Error converting start and end range to ints", "err1", err1, "err2", err2)
		return nil
	}

	listLength := h.Store.ListLength(lc.Key)

	lc.Start = h.FormatListRangeIndex(start, listLength)
	lc.End = h.FormatListRangeIndex(end, listLength)

	listArray := h.Store.ListRange(lc)
	resp := h.Encoder.GenerateArray(listArray)

	return resp
}

func (h *Handler) FormatListRangeIndex(i, len int) int {
	if i < 0 {
		return max((len + i), 0)
	}

	return min(len-1, i)
}
