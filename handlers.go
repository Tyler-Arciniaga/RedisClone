package main

import (
	"bytes"
	"log/slog"
	"net"
	"strconv"
	"sync"
)

//TODO handle error check for incorrect arg length for a given command

type Handler struct {
	Store              *Store
	Encoder            Encoder
	ClientCommandQueue map[net.Conn][]Command
	CommandQueueLock   sync.Mutex
	SubscriberChannels SubscriberChannels
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

func (h *Handler) HandleTypeCommand(cmd Command) []byte {
	key := string(cmd.Args[0])
	nativeType := h.Store.DetermineDataType(key)
	return h.Encoder.GenerateTypeString(nativeType)
}

func (h *Handler) HandleSetCommand(cmd Command) []byte {
	options := h.ParseOptions(cmd)
	sr := SetRequest{Key: string(cmd.Args[0]), Value: cmd.Args[1], Options: options}
	_, err := h.Store.SetKeyVal(sr)
	if err != nil {
		return h.Encoder.GenerateSimpleError(err.Error())
	}

	return h.Encoder.GetSimpleStringOk()
}

func (h *Handler) ParseOptions(cmd Command) []Option {
	var options []Option
	switch cmd.Name {
	case "SET":
		exOption := []byte("EX")
		optionPortion := cmd.Args[2:]
		for i := range len(optionPortion) {
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
	v, err := h.Store.GetKeyVal(key)
	if err != nil {
		return h.Encoder.GenerateSimpleError(err.Error())
	}
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

	listLength, err := h.Store.ListPush(lc)
	if err != nil {
		return h.Encoder.GenerateSimpleError(err.Error())
	}
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

	listLength, err := h.Store.ListLength(lc.Key)
	if err != nil {
		return h.Encoder.GenerateSimpleError(err.Error())
	}

	lc.Start = h.FormatListRangeIndex(start, listLength)
	lc.End = h.FormatListRangeIndex(end, listLength)

	listArray, err := h.Store.ListRange(lc)
	if err != nil {
		return h.Encoder.GenerateSimpleError(err.Error())
	}
	isForTransaction := false
	resp := h.Encoder.GenerateArray(listArray, isForTransaction)

	return resp
}

func (h *Handler) FormatListRangeIndex(i, len int) int {
	if i < 0 {
		return max((len + i), 0)
	}

	return min(len-1, i)
}

func (h *Handler) HandleListLengthCommand(cmd Command) []byte {
	key := string(cmd.Args[0])

	listLength, err := h.Store.ListLength(key)
	if err != nil {
		return h.Encoder.GenerateSimpleError(err.Error())
	}
	resp := h.Encoder.GenerateInt(listLength)

	return resp
}

func (h *Handler) HandleListPopCommand(cmd Command) []byte {
	var lc ListPopRequest
	key := string(cmd.Args[0])
	lc.Name = cmd.Name
	lc.Key = key
	lc.Count = 1
	if len(cmd.Args) > 1 {
		count, err := strconv.Atoi(string(cmd.Args[1]))
		if err != nil {
			slog.Error("Error converting pop count to int", "err", err)
		}
		lc.Count = count
	}

	listArray, err := h.Store.ListPop(lc)
	if err != nil {
		return h.Encoder.GenerateSimpleError(err.Error())
	}

	var resp []byte
	if listArray == nil {
		resp = h.Encoder.GenerateNilBulkString()
	} else if len(listArray) == 1 {
		resp = h.Encoder.GenerateBulkString(listArray[0])
	} else {
		isForTransaction := false
		resp = h.Encoder.GenerateArray(listArray, isForTransaction)
	}

	return resp
}

func (h *Handler) HandleListBlockingPopCommand(cmd Command) []byte {
	var keys []string
	for _, v := range cmd.Args[:len(cmd.Args)-1] {
		keys = append(keys, string(v))
	}
	timeout, err := strconv.ParseFloat(string(cmd.Args[len(cmd.Args)-1]), 64)
	if err != nil {
		slog.Error("Error converting timeout to float64", "err", err)
	}

	listArray, err := h.Store.ListBlockedPop(BlockedListPopRequest{Name: cmd.Name, Keys: keys, Timeout: timeout})
	if err != nil {
		return h.Encoder.GenerateSimpleError(err.Error())
	}

	var resp []byte
	if listArray == nil {
		resp = h.Encoder.GenerateNilBulkString()
	} else {
		ifForTransaction := false
		resp = h.Encoder.GenerateArray(listArray, ifForTransaction)
	}

	return resp
}

// Transaction Commands
func (h *Handler) HandleIncrCommand(cmd Command) []byte {
	key := string(cmd.Args[0])
	val, err := h.Store.IncrementKey(key)
	if err != nil {
		return h.Encoder.GenerateSimpleError(err.Error())
	}

	return h.Encoder.GenerateInt(int(val))
}

func (h *Handler) HandleMultiCommand(cmd Command) []byte {
	return h.Encoder.GetSimpleStringOk()
}

func (h *Handler) QueueCommand(cmd Command, conn net.Conn) []byte {
	h.CommandQueueLock.Lock()
	defer h.CommandQueueLock.Unlock()

	q, ok := h.ClientCommandQueue[conn]
	if !ok {
		q = []Command{}
	}
	q = append(q, cmd)

	h.ClientCommandQueue[conn] = q

	return h.Encoder.GetSimpleStringQueued()
}

func (h *Handler) GetCommandQueue(conn net.Conn) []Command {
	h.CommandQueueLock.Lock()
	defer h.CommandQueueLock.Unlock()

	q, ok := h.ClientCommandQueue[conn]
	if !ok {
		return nil
	}

	delete(h.ClientCommandQueue, conn)

	return q
}

func (h *Handler) DiscardCommandQueue(conn net.Conn) []byte {
	h.CommandQueueLock.Lock()
	defer h.CommandQueueLock.Unlock()

	_, ok := h.ClientCommandQueue[conn]
	if ok {
		delete(h.ClientCommandQueue, conn)
	}

	return h.Encoder.GetSimpleStringOk()

}

// Replication Commands
func (h *Handler) HandleInfoCommand(cmd Command, serverInfo map[string]any) []byte {
	var req InfoRequest //defaults to all booleans being false

	serverInfo["blocked_clients"] = h.Store.GetNumBlockedClients() //need to fetch this info seperately from store

	if len(cmd.Args) > 0 {
		switch string(cmd.Args[0]) {
		case "server":
			req.hasServer = true
		case "client":
			req.hasClient = true
		case "replication":
			req.hasReplication = true
		}
	} else {
		req = InfoRequest{hasServer: true, hasClient: true, hasReplication: true}
	}

	req.serverInfo = serverInfo

	return h.Encoder.GetSysInfo(req)
}

func (h *Handler) HandleReplicaOfCommand(cmd Command) ReplicaRequest {
	var req ReplicaRequest
	if len(cmd.Args) == 1 {
		//client sent host port, therefore it wants to create replica
		req.isNowMaster = false
		req.masterPort = string(cmd.Args[0])
	} else {
		//client sent REPLICAOF NO ONE, therefore it wants to establish  as master
		req.isNowMaster = true
	}
	return req
}

func (h *Handler) HandleReplicaConfigCommand(cmd Command) ([]byte, []string) {
	return h.Encoder.GetSimpleStringOk(), []string{string(cmd.Args[0]), string(cmd.Args[1])}
}

func (h *Handler) HandlePsyncCommand(cmd Command, localReplID string, localReplOffset uint64, commandBacklog *CommandBacklog) ([]byte, bool) {
	replID := cmd.Args[0]
	replOffset, _ := strconv.Atoi(string(cmd.Args[1]))

	if string(replID) == localReplID && commandBacklog.HasNeededBytes(uint64(replOffset)) {
		// this replica was once connected to local master server -> partial resync
		needsFullResync := false
		return h.Encoder.GeneratePartialResyncResp(), needsFullResync
	} else {
		// this is a new replica or command backlog does not have enough history for partial resync -> full resync
		needsFullResync := true
		return h.Encoder.GenerateFullResyncResp(localReplID, localReplOffset), needsFullResync
	}
}

// Pub Sub Commands
func (h *Handler) HandleSubscribeCommand(cmd Command, conn net.Conn) uint64 {
	numChannelsIn := h.getNumberOfSubscribedChannels(conn)

	for _, chanName := range cmd.Args {
		stringName := string(chanName)
		exists := h.SubscriberChannels.AddSubscriber(conn, stringName)
		if !exists {
			numChannelsIn++
			subscribeMsg := SubscriptionMessage{IsSubscribeMessage: true, ChanName: chanName, CurrNumChannels: numChannelsIn}
			msg := h.Encoder.GenerateSubscriptionMessage(subscribeMsg)
			h.SubscriberChannels.PublishDirectMessage(msg, stringName, conn)
		}
	}

	return numChannelsIn
}

func (h *Handler) HandleUnsubscribeCommand(cmd Command, conn net.Conn) uint64 {
	numChannelsIn := h.getNumberOfSubscribedChannels(conn)

	if len(cmd.Args) == 0 {
		for chanName := range h.SubscriberChannels.Channels {
			h.unsubscribeFromChannel(&numChannelsIn, conn, chanName)
		}
	} else {
		for _, chanName := range cmd.Args {
			stringName := string(chanName)
			h.unsubscribeFromChannel(&numChannelsIn, conn, stringName)
		}
	}

	return numChannelsIn
}

func (h *Handler) unsubscribeFromChannel(numChannelsIn *uint64, conn net.Conn, chanName string) {
	if exists := h.SubscriberChannels.RemoveSubscriber(conn, chanName); exists {
		*numChannelsIn--
		unsubscribeMsg := SubscriptionMessage{IsSubscribeMessage: false, ChanName: []byte(chanName), CurrNumChannels: *numChannelsIn}
		msg := h.Encoder.GenerateSubscriptionMessage(unsubscribeMsg)
		h.SubscriberChannels.PublishDirectMessage(msg, chanName, conn)
	}
}

func (h *Handler) getNumberOfSubscribedChannels(conn net.Conn) uint64 {
	return h.SubscriberChannels.GetNumSubscribedChan(conn)
}

func (h *Handler) HandleSubscribedPingCommand(cmd Command) []byte {
	arg := []byte("")
	if len(cmd.Args) > 0 {
		arg = cmd.Args[0]
	}

	array := [][]byte{[]byte("PONG"), arg}
	return h.Encoder.GenerateArray(array, false)
}

func (h *Handler) HandlePublishCommand(cmd Command) []byte {
	if len(cmd.Args) != 2 {
		return h.Encoder.GenerateSimpleError("ERR must specify channel and message for PUBLISH command")
	}

	chanName := cmd.Args[0]
	payload := cmd.Args[1]
	array := [][]byte{[]byte("message"), chanName, payload}

	msg := h.Encoder.GenerateArray(array, false)
	numRecieved := h.SubscriberChannels.PublishMessage(msg, string(chanName))

	return h.Encoder.GenerateInt(int(numRecieved))
}

// Sorted Sets (ZSets) Commands
func (h *Handler) HandleZAddCommand(cmd Command) []byte {
	return nil
}
