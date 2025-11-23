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
type ListRequest struct {
	Name   string
	Key    string
	Values [][]byte
}
