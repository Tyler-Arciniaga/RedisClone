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

func (zs *ZSet) ZAdd(m string, s float64) {
	if _, ok := zs.Hashmap[m]; ok {
		//TODO delete old skip list node (with old score)
	}

	zs.Hashmap[m] = s
	zs.SkipList.AddNode(m, s)
}
