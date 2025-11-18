package main

import "sync"

type Store struct {
	store map[string][]byte
	lock  sync.Mutex
}

func (s *Store) SetKeyVal(k, v []byte) bool {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.store[string(k)] = v
	return true
}

func (s *Store) GetKeyVal(k []byte) []byte {
	s.lock.Lock()
	defer s.lock.Unlock()
	v, ok := s.store[string(k)]
	if !ok {
		return nil
	}
	return v
}
