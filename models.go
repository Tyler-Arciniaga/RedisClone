package main

import "time"

type StoreData struct {
	data []byte
	ttl  time.Time
}
