package main

import (
	"bytes"
	"fmt"
	"slices"
)

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
			newNode := TrieNode{prefix: ms[i:], isEnd: true, children: make(map[byte]*TrieNode), entries: []*StreamEntry{s}} // don't need to specify an entry sequence number because defaults to 0 which is valid here
			node.children[key] = &newNode
			return
		}

		node = node.children[key]

		prefixLen := r.ComparePrefixes(ms[i:], node.prefix)
		i += prefixLen

		if prefixLen == len(node.prefix) && i == len(ms) {
			if s.ID.SequenceNum == 0 {
				s.ID.SequenceNum = node.entries[len(node.entries)].ID.SequenceNum + 1
			}
			node.entries = append(node.entries, s)
			return
		}

		if prefixLen > 0 && prefixLen < len(node.prefix) {
			newChild := TrieNode{prefix: node.prefix[prefixLen:]}
			newChild.isEnd = node.isEnd
			newChild.entries = node.entries
			node.prefix = node.prefix[:prefixLen]

			newChild.children = make(map[byte]*TrieNode)
			for k, v := range node.children {
				newChild.children[k] = v
			}
			node.children = make(map[byte]*TrieNode)
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

func (r *RadixTree) RangeOverRadixTree(StartID EntryID, EndID EntryID) ([][]byte, error) {
	entries := make([]*StreamEntry, 0)
	r.Traverse(r.root, []byte{}, StartID, EndID, &entries)

	fmt.Println(entries)

	//TODO convert e which is slice of pointers to stream entries to plain slice of bytes

	return nil, nil
}

func (r *RadixTree) Traverse(node *TrieNode, accumulatedPrefix []byte, StartID EntryID, EndID EntryID, e *[]*StreamEntry) {
	if node == nil {
		return
	}
	minMS := slices.Clone(accumulatedPrefix)
	maxMS := slices.Clone(accumulatedPrefix)

	for range 8 - len(accumulatedPrefix) {
		minMS = append(minMS, 0x00)
		maxMS = append(maxMS, 0xff)
	}

	if cmp := bytes.Compare(maxMS, StartID.Base); cmp == -1 {
		// maxMS is less than the startID thus we can prune this subtree and skip directly to the next child key
		return
	}

	if cmp := bytes.Compare(minMS, EndID.Base); cmp == 1 {
		// minMS is greater than endID therefore we haver reached the end of our traversal
		return
	}

	if len(node.entries) != 0 {
		b1 := bytes.Equal(accumulatedPrefix, StartID.Base)
		b2 := bytes.Equal(accumulatedPrefix, EndID.Base)
		if !b1 && !b2 {
			//accumulated prefix is not equal to either start or end base IDs
			*e = append(*e, node.entries...)
		}

		if b1 {
			//TODO handle adding correct entries based on seq num here
		}

		if b2 {
			//TODO handle adding correct entries based on seq num here
		}
	}

	var keys []byte
	for key := range node.children {
		keys = append(keys, key)
	}

	slices.Sort(keys) //sorts children keys in place

	for _, key := range keys {
		child := node.children[key]

		newAccumulatedPrefix := slices.Clone(accumulatedPrefix)
		newAccumulatedPrefix = append(newAccumulatedPrefix, key)

		// child.prefix already includes key[0], so we must skip it...
		if len(child.prefix) > 1 {
			newAccumulatedPrefix = append(newAccumulatedPrefix, child.prefix[1:]...)
		}

		r.Traverse(child, newAccumulatedPrefix, StartID, EndID, e)
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
