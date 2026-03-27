package main

import "math"

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

func (zs *ZSet) ZRank(m string) (int, bool) {
	score, ok := zs.Hashmap[m]
	if !ok {
		return 0, false // member does not exist in the set
	}

	node := zs.SkipList.SearchByNode(m, score)
	return node.rank, true
}

func (zs *ZSet) GetRankRange(start, end int) []MemberPair {
	startNode := zs.SkipList.SearchByRank(start)
	if startNode.Score == math.Inf(1) {
		// startNode is right boundary meaning that no nodes exist within specified score rank
		return nil
	}

	var inRange []MemberPair
	for range end - start + 1 {
		inRange = append(inRange, MemberPair{Member: startNode.Member, Score: startNode.Score})
		startNode = startNode.RightNei
		if startNode.Score == math.Inf(1) {
			break
		}
	}

	return inRange
}
