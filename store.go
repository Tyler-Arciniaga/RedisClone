package main

import (
	"sync"
	"time"
)

type Store struct {
	store map[string]StoreData
	lock  sync.Mutex
}

func (s *Store) SetKeyVal(k []byte, v StoreData) bool {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.store[string(k)] = v
	return true
}

func (s *Store) GetKeyVal(k []byte) []byte {
	s.lock.Lock()
	defer s.lock.Unlock()
	key := string(k)
	v, ok := s.store[key]
	if !ok || (!v.ttl.IsZero() && time.Now().After(v.ttl)) {
		delete(s.store, key)
		return nil
	}
	return v.data
}
