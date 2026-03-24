package main

import (
	"math"
	"math/rand"
)

type slNode struct {
	LeftNei   *slNode
	RightNei  *slNode
	TopNei    *slNode
	BottomNei *slNode

	Member string
	Score  float64
}

type SkipList struct {
	StartNode *slNode
}

type ZSet struct {
	Hashmap  map[string]float64
	SkipList SkipList
}

// return true if A less than B, return False if B less than A (don't need to handle equal since set is unique)
func ZMemberCmpr(memberA string, scoreA float64, memberB string, scoreB float64) bool {
	if scoreA <= scoreB {
		return true
	} else if scoreB < scoreA {
		return false
	} else {
		return memberA < memberB
	}
}

func NewSkipList() *SkipList {
	leftBound := slNode{Score: math.Inf(-1)}
	rightBound := slNode{Score: math.Inf(1)}

	leftBound.RightNei = &rightBound
	rightBound.LeftNei = &leftBound

	skipList := SkipList{StartNode: &leftBound}
	return &skipList
}

func FlipCoin() bool {
	if rand.Intn(2) == 0 {
		return true
	}
	return false
}

func (s *SkipList) AddNode(member string, score float64) {
	newNode := slNode{Member: member, Score: score}
	startNode := s.SearchByNode(member, score)

	endNode := startNode.RightNei
	newNode.LeftNei = startNode
	newNode.RightNei = endNode
	startNode.RightNei = &newNode
	endNode.LeftNei = &newNode

	for FlipCoin() {
		aboveNode := slNode{Member: member, Score: score}
		newNode.TopNei = &aboveNode
		aboveNode.BottomNei = &newNode

		for startNode.TopNei == nil {
			startNode = startNode.LeftNei
		}

		startNode = startNode.TopNei
		newNode = *newNode.TopNei
	} // probabilistic 50% chance of adding node to above level
}

func (s *SkipList) SearchByScore(score float64) *slNode {
	currNode := s.StartNode
	for currNode.BottomNei != nil {
		currNode = currNode.BottomNei

		// scan as far into this level as possible
		for currNode.RightNei.Score <= score {
			if currNode.Score == score {
				break // try to proceed to next bottom level
			}
			currNode = currNode.RightNei
		}
	}

	return currNode
}

func (s *SkipList) SearchByNode(member string, score float64) *slNode {
	// currNode := s.StartNode
	// for currNode.BottomNei != nil {
	// 	currNode = currNode.BottomNei
	// 	nextNode := currNode.RightNei
	//
	// 	for ZMemberCmpr(nextNode.Member, nextNode.Score, member, score) {
	// 		// while nextNode is less than or equal to searchNode...
	// 		currNode = nextNode
	// 		nextNode = nextNode.RightNei
	// 	}
	// }
	//
	// return currNode
	return nil
}
