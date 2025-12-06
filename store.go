package main

import (
	"container/list"
	"context"
	"errors"
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
	s.lock.RLock()
	defer s.lock.RUnlock()

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

	// *** Push all elements first (this is the way of handling variadic pushes since Redis 2.6) *** //
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

	//Handle client queue and possibly update the local list
	list = s.HandleClientQueue(lc.Key, list)

	//store list back into map
	s.store[lc.Key] = RedisObject{NativeType: List, Data: list}

	return list.Length, nil
}

func (s *Store) HandleClientQueue(key string, list ListData) ListData {
	if clientQueue, ok := s.listClientQueue[key]; ok && clientQueue.Len() > 0 {
		//while there is still clients waiting
		for clientQueue.Len() > 0 {
			elt := clientQueue.Front()
			waiter := elt.Value.(*Waiter)
			updatedList, poppedElt := s.UnsafeInternalListPop(list, 1, waiter.PopType)
			list = updatedList
			if poppedElt == nil {
				return list
			}

			//only once we have confirmed that an element was popped from the list do we pop from the client queue...
			clientQueue.Remove(elt)
			clientChan := waiter.ResponseChan

			clientChan <- [][]byte{[]byte(key), poppedElt[0]}
			close(clientChan)

			s.CleanUpQueueWaiters(waiter) //clean up waiters from all other queues it is in
		}
	}

	return list
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

	// Evoke internal list pop (unsafe, does not hold any lock)
	updatedList, elements := s.UnsafeInternalListPop(list, lc.Count, lc.Name)
	if updatedList.Length == 0 {
		s.DeleteKey(lc.Key)
		return elements, nil
	}

	s.store[lc.Key] = RedisObject{NativeType: List, Data: updatedList}
	return elements, nil
}

// Note: This function is unsafe, it should only ever be called by a function who holds a lock
func (s *Store) UnsafeInternalListPop(list ListData, count int, popType string) (ListData, [][]byte) {
	var elements [][]byte

	for range count {
		if list.Length == 0 {
			return list, elements
		}
		switch popType {
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
	}

	return list, elements
}

// Note: This function is unsafe, it should only ever be called by a function who holds a lock
func (s *Store) AddClientToQueue(key string, w *Waiter) *list.Element {
	queue := s.listClientQueue[key]
	if queue == nil {
		queue = list.New()
	}
	p := queue.PushBack(w)
	s.listClientQueue[key] = queue
	return p //returns pointer to waiter object in key's queue
}

// Note: This function is unsafe, it should only ever be called by a function who holds a lock
func (s *Store) CleanUpQueueWaiters(w *Waiter) {
	for key, val := range w.CleanUpPointers {
		list := s.listClientQueue[key]
		list.Remove(val)
		s.listClientQueue[key] = list
	}
}

func (s *Store) ListBlockedPop(lc BlockedListPopRequest) ([][]byte, error) {
	s.lock.Lock()

	var elements [][]byte

	clientChan := make(chan ([][]byte))
	popCommandName := lc.Name[1:]

	w := &Waiter{ResponseChan: clientChan, PopType: popCommandName, Satisfied: false, CleanUpPointers: make(map[string]*list.Element)}

	for _, key := range lc.Keys {
		list, ok, err := s.GetAsList(key)
		if err != nil {
			s.CleanUpQueueWaiters(w)

			s.lock.Unlock()
			return nil, err
		}

		// there is data in one of the requested lists...
		if ok {
			list, popped := s.UnsafeInternalListPop(list, 1, w.PopType)
			s.store[key] = RedisObject{NativeType: List, Data: list}
			s.CleanUpQueueWaiters(w)

			s.lock.Unlock()
			return [][]byte{[]byte(key), popped[0]}, nil
		} else {
			// add client to client queue for that specific list
			nodePtr := s.AddClientToQueue(key, w)
			w.CleanUpPointers[key] = nodePtr
		}
	}

	// dont continue holding lock after manipulating map
	s.lock.Unlock()

	if lc.Timeout != 0 {
		ctx, cancel := context.WithTimeout(context.TODO(), time.Duration(lc.Timeout)*time.Second)
		defer cancel()

		select {
		case elements = <-clientChan:
			s.CallCleanUpWaiters(w)
			return elements, nil
		case <-ctx.Done():
			s.CallCleanUpWaiters(w)
			return nil, nil
		}
	} else {
		elements := <-clientChan
		return elements, nil
	}
}

// Helper function to call clean up queue waiters (must aquire lock since CleanUpQueueWaiters() is unsafe)
func (s *Store) CallCleanUpWaiters(w *Waiter) {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.CleanUpQueueWaiters(w)
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
