package main

import (
	"sync"
	"time"
)

type Store struct {
	kvStore   map[string]StoreData
	listStore map[string]List
	lock      sync.Mutex
}

//TODO different mutexes for different stores

func (s *Store) SetKeyVal(r SetRequest) bool {
	s.lock.Lock()
	defer s.lock.Unlock()

	var sd StoreData
	sd.data = r.Value

	for _, v := range r.Options {
		switch v.Name {
		case "EX":
			sd.ttl = time.Now().Add(time.Duration(v.Arg.(int)) * time.Second)
		}
	}

	s.kvStore[r.Key] = sd
	return true
}

func (s *Store) GetKeyVal(key string) []byte {
	s.lock.Lock()
	defer s.lock.Unlock()

	v, ok := s.kvStore[key]
	if !ok || (!v.ttl.IsZero() && time.Now().After(v.ttl)) {
		delete(s.kvStore, key)
		return nil
	}
	return v.data
}

func (s *Store) ListPush(lc ListModificationRequest) int {
	s.lock.Lock()
	defer s.lock.Unlock()

	list, ok := s.listStore[lc.Key]
	if !ok {
		list = List{Head: nil, Tail: nil, Length: 0}
	} //create a new list

	for _, v := range lc.Values {
		switch lc.Name {
		case "LPUSH":
			newNode := ListNode{Data: v, Next: list.Head, Prev: nil}
			list.Head = &newNode
			if list.Length == 0 {
				list.Tail = &newNode
			}

		case "RPUSH":
			newNode := ListNode{Data: v, Next: nil, Prev: list.Tail}
			list.Tail = &newNode
			if list.Length == 0 {
				list.Head = &newNode
			}
		}

		list.Length += 1
	}

	s.listStore[lc.Key] = list

	return list.Length
}

func (s *Store) ListRange(lc ListRangeRequest) [][]byte {
	s.lock.Lock()
	defer s.lock.Unlock()

	var elements [][]byte
	start, end := lc.Start, lc.End

	list, ok := s.listStore[lc.Key]

	if !ok || list.Length == 0 || (start > end) {
		//return empty array (nil representation)
		return nil
	}

	ptr := list.Head
	for range lc.Start {
		ptr = ptr.Next
	}

	for range lc.End - lc.Start + 1 {
		elements = append(elements, ptr.Data)
		ptr = ptr.Next
	}

	return elements
}

func (s *Store) ListLength(key string) int {
	s.lock.Lock()
	defer s.lock.Unlock()

	list, ok := s.listStore[key]
	if !ok {
		return 0
	}
	return list.Length
}
