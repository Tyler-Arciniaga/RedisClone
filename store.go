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

func (s *Store) ListLeftPush(lc ListRequest) int {
	s.lock.Lock()
	defer s.lock.Unlock()

	list, ok := s.listStore[lc.Key]
	if !ok {
		list = List{Head: nil, Tail: nil, Length: 0}
	} //create a new list

	for _, v := range lc.Values {
		newNode := ListNode{Data: v, Next: list.Head, Prev: nil}
		list.Head = &newNode

		if list.Length == 0 {
			list.Tail = &newNode
		}

		list.Length += 1
	}

	s.listStore[lc.Key] = list

	return list.Length
}
