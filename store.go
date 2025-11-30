package main

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

type Store struct {
	kvStore           map[string]StoreData
	listStore         map[string]List
	listClientQueue   map[string][]chan ([][]byte)
	closedClientChans map[chan ([][]byte)]bool
	lock              sync.RWMutex
}

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

	go s.ScanClientQueue(lc.Key)

	return list.Length
}

func (s *Store) ScanClientQueue(key string) {
	for len(s.listClientQueue[key]) > 0 && s.listStore[key].Length > 0 {
		queue, ok := s.listClientQueue[key]
		if !ok || len(queue) == 0 {
			return
		}

		var poppedClientChan chan ([][]byte)
		for len(queue) > 0 {
			poppedClientChan = queue[0]
			if len(queue) > 1 {
				queue = queue[1:]
			} else {
				queue = queue[:0]
			}
			s.listClientQueue[key] = queue

			if _, exists := s.closedClientChans[poppedClientChan]; exists {
				continue
			}
			break
		}

		if _, exists := s.closedClientChans[poppedClientChan]; exists {
			return
		}

		poppedElement := s.ListPop(ListPopRequest{Name: "LPOP", Key: key, Count: 1})[0]

		poppedClientChan <- [][]byte{[]byte(key), poppedElement}

		s.AppendToClosedClientSet(poppedClientChan)
		close(poppedClientChan) //writer always closes channel, not reader!
	}
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
		if list.Length == 0 {
			delete(s.listStore, lc.Key)
			return elements
		}
	}

	s.listStore[lc.Key] = list
	return elements
}

func (s *Store) ListBlockedPop(lc ListBlockedPopRequest) [][]byte {
	var elements [][]byte
	clientChan := make(chan ([][]byte))

	for _, key := range lc.Keys {
		if _, ok := s.listStore[key]; ok {
			poppedKey := key
			popped := s.ListPop(ListPopRequest{Name: "LPOP", Key: key, Count: 1})[0]
			return [][]byte{[]byte(poppedKey), popped}
		} else {
			s.lock.Lock()
			queue := s.listClientQueue[key]
			queue = append(queue, clientChan)
			s.listClientQueue[key] = queue
			s.lock.Unlock()
		}
	}

	if lc.Timeout != 0 {
		ctx, cancel := context.WithTimeout(context.TODO(), time.Duration(lc.Timeout)*time.Second)
		defer cancel()

		select {
		case elements = <-clientChan:
			s.AppendToClosedClientSet(clientChan)
			return elements
		case <-ctx.Done():
			s.AppendToClosedClientSet(clientChan)
			return nil
		}
	} else {
		elements := <-clientChan
		return elements
	}
}

func (s *Store) AppendToClosedClientSet(c chan ([][]byte)) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.closedClientChans[c] = true
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
