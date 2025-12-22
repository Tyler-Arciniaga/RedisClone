package main

import "fmt"

// Stream implemented using a Radix tree (space optimized Prefix Trie)
type TrieNode struct {
	prefix   []byte
	isEnd    bool
	children map[byte]*TrieNode
	entries  []*StreamEntry
}

type RadixTree struct {
	root *TrieNode
}

func (r *RadixTree) ComparePrefixes(a, b []byte) int {
	var length int
	aLen := len(a)
	bLen := len(b)
	for length < aLen && length < bLen && a[length] == b[length] {
		length++
	}

	return length
}

func (r *RadixTree) Insert(s *StreamEntry) {
	ms := s.ID.Base
	fmt.Println(ms)
	node := r.root
	i := 0

	for i < len(ms) {
		fmt.Println("loop")
		key := ms[i]
		_, childExists := node.children[key]

		if !childExists {
			newNode := TrieNode{prefix: ms[i:], isEnd: true, children: make(map[byte]*TrieNode), entries: []*StreamEntry{s}}
			fmt.Println("here", newNode)
			node.children[key] = &newNode
			fmt.Println(node)
			return
		}

		node = node.children[key]
		fmt.Println("here3", node)

		prefixLen := r.ComparePrefixes(ms[i:], node.prefix)
		i += prefixLen

		if prefixLen == len(node.prefix) && i == len(ms) {
			node.entries = append(node.entries, s)
			//TODO update s.seqNum if the user did not specify an ID
			fmt.Println(node.entries)
			return
		}

		if prefixLen < len(node.prefix) {
			fmt.Println("here1")
			newChild := TrieNode{prefix: node.prefix[prefixLen:]}
			newChild.isEnd = node.isEnd
			newChild.children = node.children
			newChild.entries = node.entries
			node.prefix = node.prefix[:prefixLen]
			node.children[newChild.prefix[0]] = &newChild
			node.isEnd = i == len(ms)
			if node.isEnd {
				node.entries = []*StreamEntry{s}
			} else {
				node.entries = node.entries[:0] //clear the old node's entries
			}
		}
	}
}

// func (r *RadixTree) Search(id []byte) (StreamEntry, bool) {
// 	node := r.root
// 	i := 0

// 	for i < len(id) {
// 		key := id[i]
// 		if _, ok := node.children[key]; !ok {
// 			return StreamEntry{}, false
// 		}

// 		node = node.children[key]

// 		prefixLen := r.ComparePrefixes(id[i:], node.prefix)
// 		if prefixLen != len(node.prefix) {
// 			return StreamEntry{}, false
// 		}

// 		i += prefixLen

// 		if i == prefixLen {
// 			if node.isEnd {
// 				return *node.entry, true
// 			}
// 			return StreamEntry{}, false
// 		}
// 	}
// 	return StreamEntry{}, false
// }
