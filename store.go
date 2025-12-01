package main

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"
)

type Store struct {
	store             map[string]RedisObject
	listClientQueue   map[string][]BlockedPopQueueItem
	closedClientChans map[chan ([][]byte)]bool
	lock              sync.RWMutex
}

func (s *Store) GetAsBytes(key string) (KV_Data, bool, error) {
	s.lock.RLock()
	defer s.lock.RUnlock()

	obj, ok := s.store[key]
	if !ok {
		return KV_Data{}, false, nil
	}

	kv, ok := obj.Data.(KV_Data)
	if obj.NativeType != Bytes || !ok {
		return KV_Data{}, true, errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	return kv, true, nil
}

func (s *Store) GetAsList(key string) (ListData, bool, error) {
	s.lock.RLock()
	defer s.lock.RUnlock()

	obj, ok := s.store[key]
	if !ok {
		return ListData{}, false, nil
	}

	list, ok := obj.Data.(ListData)
	if obj.NativeType != List || !ok {
		return ListData{}, true, errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	return list, true, nil
}

func (s *Store) SetKeyVal(r SetRequest) bool {
	s.lock.Lock()
	defer s.lock.Unlock()

	var kv KV_Data
	kv.Data = r.Value

	for _, v := range r.Options {
		switch v.Name {
		case "EX":
			kv.TTL = time.Now().Add(time.Duration(v.Arg.(int)) * time.Second)
		}
	}

	obj := RedisObject{NativeType: Bytes, Data: kv}

	s.store[r.Key] = obj
	return true
}

func (s *Store) GetKeyVal(key string) []byte {

	kvData, ok, err := s.GetAsBytes(key)

	if !ok {
		return nil
	}
	if err != nil {
		slog.Error(err.Error()) //TODO refactor once error functionality made (and everywhere else)
	}

	if !kvData.TTL.IsZero() && time.Now().After(kvData.TTL) {
		delete(s.store, key)
		return nil
	}
	return kvData.Data
}

func (s *Store) ListPush(lc ListModificationRequest) int {
	list, ok, err := s.GetAsList(lc.Key)
	if !ok {
		list = ListData{Head: nil, Tail: nil, Length: 0} //create a new list
	} else {
		if err != nil {
			slog.Error(err.Error())
		}
	}

	s.lock.Lock()
	defer s.lock.Unlock()

	for _, v := range lc.Values {
		switch lc.Name {
		case "LPUSH":
			newNode := ListNode{Data: v, Next: list.Head, Prev: nil}
			if list.Head != nil {
				list.Head.Prev = &newNode
			}
			list.Head = &newNode
			if list.Length == 0 {
				list.Tail = &newNode
			}

		case "RPUSH":
			newNode := ListNode{Data: v, Next: nil, Prev: list.Tail}
			if list.Tail != nil {
				list.Tail.Next = &newNode
			}

			list.Tail = &newNode

			if list.Length == 0 {
				list.Head = &newNode
			}
		}

		list.Length += 1
	}

	s.store[lc.Key] = RedisObject{NativeType: List, Data: list}

	go s.ScanClientQueue(lc.Key)

	return list.Length
}

// TODO refactor me to account for the generic storage
func (s *Store) ScanClientQueue(key string) {
	for len(s.listClientQueue[key]) > 0 && s.store[key].Data.(ListData).Length > 0 {
		queue, ok := s.listClientQueue[key]
		if !ok || len(queue) == 0 {
			return
		}

		var poppedClientChan chan ([][]byte)
		var popType string
		for len(queue) > 0 {
			queueItem := queue[0]
			poppedClientChan, popType = queueItem.ClientChan, queueItem.PopType
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

		poppedElement := s.ListPop(ListPopRequest{Name: popType, Key: key, Count: 1})[0]

		poppedClientChan <- [][]byte{[]byte(key), poppedElement}

		s.AppendToClosedClientSet(poppedClientChan)
		close(poppedClientChan) //writer always closes channel, not reader!
	}
}

func (s *Store) ListPop(lc ListPopRequest) [][]byte {
	list, ok, err := s.GetAsList(lc.Key)
	if !ok {
		return nil
	}
	if err != nil {
		slog.Error(err.Error())
	}

	s.lock.Lock()
	defer s.lock.Unlock()

	var elements [][]byte

	for range lc.Count {
		if list.Length == 0 {
			delete(s.store, lc.Key)
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
			if prev != nil {
				prev.Next = nil
			}
		}

		list.Length -= 1
		if list.Length == 0 {
			delete(s.store, lc.Key)
			return elements
		}
	}

	s.store[lc.Key] = RedisObject{NativeType: List, Data: list}
	return elements
}

func (s *Store) ListBlockedPop(lc ListBlockedPopRequest) [][]byte {
	var elements [][]byte
	clientChan := make(chan ([][]byte))

	popCommandName := lc.Name[1:]

	for _, key := range lc.Keys {
		if _, ok, err := s.GetAsList(key); ok && err == nil {
			popped := s.ListPop(ListPopRequest{Name: popCommandName, Key: key, Count: 1})[0]
			return [][]byte{[]byte(key), popped}
		} else {
			s.lock.Lock()
			queue := s.listClientQueue[key]
			queue = append(queue, BlockedPopQueueItem{ClientChan: clientChan, PopType: popCommandName})
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
	list, ok, err := s.GetAsList(lc.Key)
	if err != nil {
		slog.Error(err.Error())
	}

	var elements [][]byte
	start, end := lc.Start, lc.End

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
	list, ok, err := s.GetAsList(key)
	if !ok {
		return 0
	}

	if err != nil {
		slog.Error("(error) WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	return list.Length
}
