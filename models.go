package main

import "time"

type StoreData struct {
	data []byte
	ttl  time.Time
}

type ListNode struct {
	Data []byte
	Next *ListNode
	Prev *ListNode
}
type List struct {
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
