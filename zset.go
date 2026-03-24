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

	s.InsertNode(startNode, &newNode)

	for FlipCoin() {
		aboveNode := slNode{Member: member, Score: score}
		newNode.TopNei = &aboveNode
		aboveNode.BottomNei = &newNode

		for startNode.TopNei == nil && startNode.LeftNei != nil {
			startNode = startNode.LeftNei
		} // continue moving the start node back until it either reaches a node with a reference to the above level, or becomes the -inf node (therefore need to add a completely new level)

		if startNode.LeftNei == nil {
			// startNode has reached the -inf node for this level ... need to create new top level
			startNode.TopNei = s.CreateUpperLevel(startNode)
		}
		startNode = startNode.TopNei
		newNode = aboveNode

		s.InsertNode(startNode, &newNode)
	} // probabilistic 50% chance of adding node to above level
}

// insert node after previous node, correctly altering references of the two nodes it is inserted between
func (s *SkipList) InsertNode(prevNode, newNode *slNode) {
	endNode := prevNode.RightNei

	newNode.LeftNei = prevNode
	newNode.RightNei = endNode
	prevNode.RightNei = newNode
	endNode.LeftNei = newNode
}

// create new skip list level and have its left bound reference the current left bound, return ptr to new left bound
func (s *SkipList) CreateUpperLevel(currLeftBound *slNode) *slNode {
	newLeftBound := &slNode{Score: math.Inf(-1)}
	newRightBound := &slNode{Score: math.Inf(1)}

	newLeftBound.RightNei = newRightBound
	newRightBound.LeftNei = newLeftBound

	newLeftBound.BottomNei = currLeftBound

	return newLeftBound
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
