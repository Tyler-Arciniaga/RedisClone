package main

import ()

type ZSet struct {
	Hashmap    map[string]float64
	SkipList   SkipList
	NumMembers int
}

// add new member to zset, return false if member is already in zset (thus ZAdd just updates member's score)
func (zs *ZSet) ZAdd(m string, s float64) bool {
	isNew := true
	zs.NumMembers++

	if oldScore, ok := zs.Hashmap[m]; ok {
		zs.SkipList.RemoveNode(m, oldScore)
		isNew = false
		zs.NumMembers--
	}

	zs.Hashmap[m] = s
	zs.SkipList.AddNode(m, s)

	return isNew
}
