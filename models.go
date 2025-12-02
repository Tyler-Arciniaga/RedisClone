package main

import (
	"time"
)

type RedisObject struct {
	NativeType NativeType
	Data       any
}

type NativeType int

const (
	Bytes NativeType = iota
	List
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

type ListBlockedPopRequest struct {
	Name    string
	Keys    []string
	Timeout float64
}

type BlockedPopQueueItem struct {
	ClientChan chan ([][]byte)
	PopType    string
}
