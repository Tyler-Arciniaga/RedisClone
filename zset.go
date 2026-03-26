package main

import (
	"fmt"
)

type ZSet struct {
	Hashmap  map[string]float64
	SkipList SkipList
}

type slNode struct {
	LeftNei   *slNode
	RightNei  *slNode
	TopNei    *slNode
	BottomNei *slNode

	Member string
	Score  float64
}

func (n *slNode) PrintNode() {
	fmt.Printf("(%s,%g) -> ", n.Member, n.Score)
}

// add new member to zset, return false if member is already in zset (thus ZAdd just updates member's score)
func (zs *ZSet) ZAdd(m string, s float64) bool {
	isNew := true
	if oldScore, ok := zs.Hashmap[m]; ok {
		zs.SkipList.RemoveNode(m, oldScore)
		isNew = false
	}

	zs.Hashmap[m] = s
	zs.SkipList.AddNode(m, s)

	return isNew
}
