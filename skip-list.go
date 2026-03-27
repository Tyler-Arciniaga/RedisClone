package main

import (
	"fmt"
	"math"
	"math/rand"
)

type SkipList struct {
	StartNode *slNode
	NumNodes  uint64
}

type slNode struct {
	LeftNei   *slNode
	RightNei  *slNode
	TopNei    *slNode
	BottomNei *slNode

	Member string
	Score  float64
	Span   uint64
	rank   int // rank values may become stale, however, if they are ever considered we can assume they are up to date
}

func (n *slNode) PrintNode() {
	fmt.Printf("(%s,%g, %d) -> ", n.Member, n.Score, n.Span)
}

// return true if A less than B, return False if B less than A (don't need to handle equal since set is unique)
func ZMemberCmpr(memberA string, scoreA float64, memberB string, scoreB float64) bool {
	if scoreA < scoreB {
		return true
	} else if scoreB < scoreA {
		return false
	} else {
		return memberA <= memberB
	}
}

func NewSkipList() *SkipList {
	leftBound := slNode{Score: math.Inf(-1), Span: 1, rank: -1}
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
	newNode := &slNode{Member: member, Score: score}
	startNode := s.SearchByNode(member, score)
	newNode.rank = startNode.rank + 1

	s.InsertNode(startNode, newNode)
	s.UpdateUpperLevelSpans(startNode)

	for FlipCoin() {
		aboveNode := &slNode{Member: member, Score: score, rank: newNode.rank}
		newNode.TopNei = aboveNode
		aboveNode.BottomNei = newNode

		for startNode.TopNei == nil && startNode.LeftNei != nil {
			startNode = startNode.LeftNei
		} // continue moving the start node back until it either reaches a node with a reference to the above level, or becomes the -inf node (therefore need to add a completely new level)

		if startNode.LeftNei == nil && startNode.TopNei == nil {
			// startNode has reached the -inf node for this level (and no upper level exists) ... need to create new top level
			startNode.TopNei = s.CreateUpperLevel(startNode)
		}

		startNode = startNode.TopNei
		newNode = aboveNode

		s.InsertNode(startNode, newNode)
	} // probabilistic 50% chance of adding node to above level

	s.NumNodes++
}

func (s *SkipList) UpdateUpperLevelSpans(prevNode *slNode) {
	for prevNode.TopNei == nil && prevNode.LeftNei != nil {
		prevNode = prevNode.LeftNei
	}

	for prevNode.TopNei != nil {
		prevNode = prevNode.TopNei
		prevNode.Span++

		for prevNode.TopNei == nil && prevNode.LeftNei != nil {
			prevNode = prevNode.LeftNei
		}
	}
}

func (s *SkipList) RemoveNode(member string, score float64) {
	node := s.SearchByNode(member, score)
	if node.Member != member {
		return
	}

	s.RemoveNodeFromLevel(node) // remove node from bottom most level

	for node.TopNei != nil {
		node = node.TopNei
		s.RemoveNodeFromLevel(node)
	}

	s.NumNodes--
}

// insert node after previous node, correctly altering references of the two nodes it is inserted between
// assumes that prevNode and newNode have correctly updated rank values
func (s *SkipList) InsertNode(prevNode, newNode *slNode) {
	endNode := prevNode.RightNei

	newNode.LeftNei = prevNode
	newNode.RightNei = endNode
	prevNode.RightNei = newNode
	endNode.LeftNei = newNode

	newNode.Span = prevNode.Span - uint64(newNode.rank-prevNode.rank) + 1
	// if prevNode.Span != s.NumNodes+1 {
	// 	newNode.Span += 1
	// }

	prevNode.Span = uint64(newNode.rank - prevNode.rank)
}

func (s *SkipList) RemoveNodeFromLevel(node *slNode) {
	node.LeftNei.RightNei = node.RightNei
	node.RightNei.LeftNei = node.LeftNei
}

// create new skip list level and have its left bound reference the current left bound, return ptr to new left bound
func (s *SkipList) CreateUpperLevel(currLeftBound *slNode) *slNode {
	newLeftBound := &slNode{Score: math.Inf(-1), Span: s.NumNodes + 1, rank: -1}
	newRightBound := &slNode{Score: math.Inf(1)}

	newLeftBound.RightNei = newRightBound
	newRightBound.LeftNei = newLeftBound

	newLeftBound.BottomNei = currLeftBound

	s.StartNode = newLeftBound
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
	runningRank := -1

	currNode := s.StartNode
	currNode.rank = runningRank

	nextNode := currNode.RightNei

	for ZMemberCmpr(nextNode.Member, nextNode.Score, member, score) {
		// while nextNode is less than or equal to searchNode...
		runningRank += int(currNode.Span)
		currNode = nextNode
		currNode.rank = runningRank

		nextNode = nextNode.RightNei
	}

	for currNode.BottomNei != nil {
		currNode = currNode.BottomNei
		currNode.rank = currNode.TopNei.rank
		nextNode = currNode.RightNei

		for ZMemberCmpr(nextNode.Member, nextNode.Score, member, score) {
			// while nextNode is less than or equal to searchNode...
			runningRank += int(currNode.Span)
			currNode = nextNode
			currNode.rank = runningRank
			nextNode = nextNode.RightNei
		}
	}

	return currNode
}

func (s *SkipList) SearchByRank(rank int) *slNode {
	runningRank := -1
	currNode := s.StartNode

	rightBoundScore := math.Inf(1)

	for currNode.Score != rightBoundScore && runningRank+int(currNode.Span) <= rank {
		runningRank = int(currNode.Span)
		currNode = currNode.RightNei
	}

	for currNode.BottomNei != nil {
		currNode = currNode.BottomNei

		for currNode.Score != rightBoundScore && runningRank+int(currNode.Span) <= rank {
			runningRank += int(currNode.Span)
			currNode = currNode.RightNei
		}
	}

	return currNode
}

// Skip List Testing
func (s *SkipList) PrintList() {
	currLeftBound := s.StartNode

	for {
		curr := currLeftBound
		for {
			curr.PrintNode()
			if curr.RightNei == nil {
				break
			}
			curr = curr.RightNei
		}

		if currLeftBound.BottomNei == nil {
			break
		}
		fmt.Print("\n")
		currLeftBound = currLeftBound.BottomNei
	}

	fmt.Print("\n___________________________\n")
}

func TestSkipList() {
	Test1()
}

func Test1() {
	skipList := NewSkipList()

	skipList.AddNode("a", 1)
	skipList.AddNode("b", 5)
	skipList.AddNode("c", 3)
	skipList.AddNode("d", 2)
	skipList.AddNode("e", 1)
	skipList.PrintList()
}
