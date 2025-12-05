package main

import (
	"container/list"
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"
)

type Store struct {
	store             map[string]RedisObject
	listClientQueue   map[string]*list.List
	closedClientChans map[chan ([][]byte)]bool
	lock              sync.RWMutex
}

// Note: This function is unsafe, it should only ever be called by a function who holds a lock
func (s *Store) GetAsBytes(key string) (KV_Data, bool, error) {
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

// Note: This function is unsafe, it should only ever be called by a function who holds a lock
func (s *Store) GetAsList(key string) (ListData, bool, error) {
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

func (s *Store) SetKeyVal(r SetRequest) (bool, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	kv, _, err := s.GetAsBytes(r.Key)
	if err != nil {
		return false, err
	}

	kv.Data = r.Value
	for _, v := range r.Options {
		switch v.Name {
		case "EX":
			kv.TTL = time.Now().Add(time.Duration(v.Arg.(int)) * time.Second)
		}
	}

	obj := RedisObject{NativeType: Bytes, Data: kv}
	s.store[r.Key] = obj
	return true, nil
}

// Note: This function is unsafe, it should only ever be called by a function who holds a lock
func (s *Store) DeleteKey(key string) {
	delete(s.store, key)
}

func (s *Store) GetKeyVal(key string) ([]byte, error) {
	kvData, ok, err := s.GetAsBytes(key)

	if !ok {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	if !kvData.TTL.IsZero() && time.Now().After(kvData.TTL) {
		s.DeleteKey(key)
		return nil, nil
	} // Passive expiry logic

	return kvData.Data, nil
}

func (s *Store) ListPush(lc ListModificationRequest) (int, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	list, ok, err := s.GetAsList(lc.Key)
	if !ok {
		list = ListData{Head: nil, Tail: nil, Length: 0} //create a new list
	} else {
		if err != nil {
			return 0, err
		}
	}

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

	return list.Length, nil
}

// Note: This function is unsafe, it should only ever be called by a function who holds a lock
func (s *Store) ScanClientQueue(key string) {
	list, _, err := s.GetAsList(key)
	listLen := list.Length
	if err != nil {
		return
	}
	clientQueue := s.listClientQueue[key]

	for clientQueue.Len() > 0 && listLen > 0 {
		elt := clientQueue.Front()
		if elt == nil {
			break
		}
		waiter := clientQueue.Remove(elt).(*Waiter)
		if waiter.Satisfied {
			continue
		}
		waiter.Satisfied = true
		clientChan, popType := waiter.ResponseChan, waiter.PopType
		poppedElt, err := s.ListPop(ListPopRequest{Name: popType, Key: key, Count: 1})
		listLen-- //update local list length
		if err != nil {
			slog.Error(err.Error())
			return
		}

		clientChan <- [][]byte{[]byte(key), poppedElt[0]}
		close(clientChan)

		go s.CleanUpQueueWaiters(waiter)
	}

	s.lock.Lock()
	s.listClientQueue[key] = clientQueue
	s.lock.Unlock()
}

func (s *Store) ListPop(lc ListPopRequest) ([][]byte, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	list, ok, err := s.GetAsList(lc.Key)
	if !ok {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	var elements [][]byte

	for range lc.Count {
		if list.Length == 0 {
			s.DeleteKey(lc.Key)
			return elements, nil
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
			s.DeleteKey(lc.Key)
			return elements, nil
		}
	}

	s.store[lc.Key] = RedisObject{NativeType: List, Data: list}
	return elements, nil
}

// Note: This function is unsafe, it should only ever be called by a function who holds a lock
func (s *Store) AddClientToQueue(key string, w *Waiter) *list.Element {
	queue := s.listClientQueue[key]
	if queue == nil {
		queue = list.New()
	}
	p := queue.PushBack(w) //returns pointer to waiter object in key's queue
	s.listClientQueue[key] = queue
	return p
}

// TODO check me!!
func (s *Store) CleanUpQueueWaiters(w *Waiter) {
	s.lock.Lock()
	defer s.lock.Unlock()

	for key, val := range w.CleanUpPointers {
		list := s.listClientQueue[key]
		list.Remove(val)
		s.listClientQueue[key] = list
	}
}

func (s *Store) ListBlockedPop(lc BlockedListPopRequest) ([][]byte, error) {
	var elements [][]byte

	clientChan := make(chan ([][]byte))
	popCommandName := lc.Name[1:]

	w := &Waiter{ResponseChan: clientChan, PopType: popCommandName, Satisfied: false, CleanUpPointers: make(map[string]*list.Element)}

	for _, key := range lc.Keys {
		_, ok, err := s.GetAsList(key)
		if err != nil {
			return nil, err
		}
		if ok {
			popped, err := s.ListPop(ListPopRequest{Name: popCommandName, Key: key, Count: 1})
			if err != nil {
				return nil, err
			}

			go s.CleanUpQueueWaiters(w)
			return [][]byte{[]byte(key), popped[0]}, nil
		} else {
			s.lock.Lock()
			nodePtr := s.AddClientToQueue(key, w)
			w.CleanUpPointers[key] = nodePtr
			s.lock.Unlock()
		}
	}

	if lc.Timeout != 0 {
		ctx, cancel := context.WithTimeout(context.TODO(), time.Duration(lc.Timeout)*time.Second)
		defer cancel()

		select {
		case elements = <-clientChan:
			s.AppendToClosedClientSet(clientChan)
			return elements, nil
		case <-ctx.Done():
			s.AppendToClosedClientSet(clientChan)
			return nil, nil
		}
	} else {
		elements := <-clientChan
		return elements, nil
	}
}

func (s *Store) AppendToClosedClientSet(c chan ([][]byte)) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.closedClientChans[c] = true
}

func (s *Store) ListRange(lc ListRangeRequest) ([][]byte, error) {
	s.lock.RLock()
	defer s.lock.RUnlock()

	list, ok, err := s.GetAsList(lc.Key)
	if err != nil {
		return nil, err
	}

	var elements [][]byte
	start, end := lc.Start, lc.End

	if !ok || list.Length == 0 || (start > end) {
		//return empty array (nil representation)
		return nil, nil
	}

	ptr := list.Head
	for range lc.Start {
		ptr = ptr.Next
	}

	for range lc.End - lc.Start + 1 {
		elements = append(elements, ptr.Data)
		ptr = ptr.Next
	}

	return elements, nil
}

func (s *Store) ListLength(key string) (int, error) {
	s.lock.RLock()
	defer s.lock.RUnlock()

	list, ok, err := s.GetAsList(key)
	if !ok {
		return 0, nil
	}

	if err != nil {
		return 0, errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	return list.Length, nil
}
