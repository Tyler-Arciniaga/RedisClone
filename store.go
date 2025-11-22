package main

import (
	"sync"
	"time"
)

type Store struct {
	store map[string]StoreData
	lock  sync.Mutex
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

	s.store[r.Key] = sd
	return true
}

func (s *Store) GetKeyVal(key string) []byte {
	s.lock.Lock()
	defer s.lock.Unlock()

	v, ok := s.store[key]
	if !ok || (!v.ttl.IsZero() && time.Now().After(v.ttl)) {
		delete(s.store, key)
		return nil
	}
	return v.data
}
