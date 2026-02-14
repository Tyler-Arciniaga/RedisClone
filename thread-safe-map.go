package main

import "sync"

//TODO: put this and backlog in a utils folder

type SafeMap[T comparable, Y any] struct {
	data map[T]Y
	mtx  sync.RWMutex
}

func NewSafeMap[K comparable, V any]() *SafeMap[K, V] {
	return &SafeMap[K, V]{data: make(map[K]V), mtx: sync.RWMutex{}}
}

func (s *SafeMap[T, Y]) UpsertKV(key T, value Y) {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	s.data[key] = value
}

func (s *SafeMap[T, Y]) GetValue(key T) (Y, bool) {
	s.mtx.RLock()
	defer s.mtx.RUnlock()

	value, ok := s.data[key]
	return value, ok
}

// returns boolean regarding if key existed in map, boolean may be safely ignored when not needed
func (s *SafeMap[T, Y]) DeleteKey(key T) bool {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	if _, ok := s.data[key]; ok {
		delete(s.data, key)
		return true
	}

	return false
}

func (s *SafeMap[T, Y]) GetKeys() []T {
	s.mtx.RLock()
	defer s.mtx.RUnlock()

	var res []T
	for key := range s.data {
		res = append(res, key)
	}

	return res

}

func (s *SafeMap[T, Y]) GetValues() []Y {
	s.mtx.RLock()
	defer s.mtx.RUnlock()

	var res []Y
	for _, value := range s.data {
		res = append(res, value)
	}

	return res
}

func (s *SafeMap[T, Y]) GetItems() [][]any {
	s.mtx.RLock()
	defer s.mtx.RUnlock()

	var res [][]any
	for key, value := range s.data {
		res = append(res, []any{key, value})
	}

	return res
}

func (s *SafeMap[T, Y]) GetLen() int {
	s.mtx.RLock()
	defer s.mtx.RUnlock()

	return len(s.data)
}

func (s *SafeMap[T, Y]) Clear() {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	for key := range s.data {
		delete(s.data, key)
	}
}
