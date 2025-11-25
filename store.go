package main

import (
	"log/slog"
	"net"
	"sync"
	"time"
)

type Store struct {
	kvStore         map[string]StoreData
	listStore       map[string]List
	listClientQueue map[string][]net.Conn
	lock            sync.RWMutex
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
	s.lock.RLock()
	defer s.lock.RUnlock()

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
			if list.Length != 0 {
				list.Tail.Next = &newNode
			}

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

func (s *Store) ListPop(lc ListPopRequest) [][]byte {
	s.lock.Lock()
	defer s.lock.Unlock()

	var elements [][]byte

	list, ok := s.listStore[lc.Key]
	if !ok {
		return nil
	}

	for range lc.Count {
		if list.Length == 0 {
			delete(s.listStore, lc.Key)
			return elements
		}
		switch lc.Name {
		case "LPOP":
			elements = append(elements, list.Head.Data)
			nxt := list.Head.Next
			list.Head.Next = nil
			list.Head = nxt
			if list.Head != nil {
				list.Head.Prev = nil
			}
		case "RPOP":
			elements = append(elements, list.Tail.Data)
			prev := list.Tail.Prev
			list.Tail.Prev = nil
			list.Tail = prev
			if list.Tail != nil {
				list.Tail.Next = nil
			}
		}

		list.Length -= 1
	}

	s.listStore[lc.Key] = list
	return elements
}

func (s *Store) ListBlockedPop(lc ListBlockedPopRequest) [][]byte {
	var elements [][]byte

	for _, key := range lc.Keys {
		if _, ok := s.listStore[key]; ok {
			poppedKey := key
			popped := s.ListPop(ListPopRequest{Name: "LPOP", Key: key, Count: 1})[0]
			return [][]byte{[]byte(poppedKey), popped}
		} else {
			s.lock.Lock()
			q := s.listClientQueue[key]
			q = append(q, lc.Conn)
			s.listClientQueue[key] = q
			s.lock.Unlock()
		}
	}

	return elements
}

func (s *Store) ListRange(lc ListRangeRequest) [][]byte {
	s.lock.RLock()
	defer s.lock.RUnlock()

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
	s.lock.RLock()
	defer s.lock.RUnlock()

	list, ok := s.listStore[key]
	if !ok {
		if _, ok := s.kvStore[key]; ok {
			//TODO return formatted error here eventually instead of just 0
			slog.Error("(error) WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		return 0
	}
	return list.Length
}
