package main

import (
	"container/list"
	"net"
	"time"
)

// non-main-server Structs
type Node interface {
	GetConn() net.Conn
}

type ClientObject struct {
	Conn                  net.Conn
	NumSubscribedChannels uint64
}

type ReplicaObject struct {
	Conn net.Conn
}

func (c *ClientObject) GetConn() net.Conn {
	return c.Conn
}

func (r *ReplicaObject) GetConn() net.Conn {
	return r.Conn
}

// Store Structs
type RedisObject struct {
	NativeType NativeType
	Data       any
}

type NativeType int

const (
	Bytes NativeType = iota
	List
	Stream
	Z_Set
	None
)

// String Structs
type KV_Data struct {
	Data []byte
	TTL  time.Time
}

// List Structs
type ListNode struct {
	Data []byte
	Next *ListNode
	Prev *ListNode
}
type ListData struct {
	Head   *ListNode
	Tail   *ListNode
	Length int
}
type ListModificationRequest struct {
	Name   string
	Key    string
	Values [][]byte
}

type ListRangeRequest struct {
	Name  string
	Key   string
	Start int
	End   int
}

type ListPopRequest struct {
	Name  string
	Key   string
	Count int
}

// Key-Client Queue Structs
type BlockedListPopRequest struct {
	Name    string
	Keys    []string
	Timeout float64
}

type BlockedPopQueueItem struct {
	ClientChan chan ([][]byte)
	PopType    string
}

type Waiter struct {
	ResponseChan    chan ([][]byte)
	PopType         string
	Satisfied       bool
	CleanUpPointers map[string]*list.Element
}

// Replication Structs
type InfoRequest struct {
	hasServer      bool
	hasClient      bool
	hasReplication bool
	serverInfo     map[string]any
} //TODO add more INFO parameters (just has the basic ones for now)

type ReplicaRequest struct {
	isNowMaster bool
	masterPort  string
}

type PsyncResponse struct {
	isPartialResync bool
	masterID        string
	masterOffset    uint64
}

// Pub Sub Structs
type SubscriptionMessage struct {
	IsSubscribeMessage bool
	ChanName           []byte
	CurrNumChannels    uint64
}

// ZSet Structs
type MemberPair struct {
	Member string
	Score  float64
}
type ZSetModificationRequest struct {
	Key     string
	Members []MemberPair
}
