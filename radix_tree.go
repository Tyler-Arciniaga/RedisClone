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
	node := r.root
	i := 0

	for i < len(ms) {
		key := ms[i]
		_, childExists := node.children[key]

		if !childExists {
			newNode := TrieNode{prefix: ms[i:], isEnd: true, children: make(map[byte]*TrieNode), entries: []*StreamEntry{s}}
			node.children[key] = &newNode
			return
		}

		node = node.children[key]

		prefixLen := r.ComparePrefixes(ms[i:], node.prefix)
		i += prefixLen

		if prefixLen < len(node.prefix) {
			newChild := TrieNode{prefix: node.prefix[prefixLen:]}
			newChild.isEnd = node.isEnd
			newChild.children = node.children
			newChild.entries = node.entries
			node.prefix = node.prefix[:prefixLen]
			node.children[newChild.prefix[0]] = &newChild
			node.isEnd = i == len(ms)
			if node.isEnd {
				node.entries = append(node.entries, s)
				fmt.Println(node.entries)
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
