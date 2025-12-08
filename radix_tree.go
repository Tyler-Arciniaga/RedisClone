package main

// Stream implemented using a Radix tree (space optimized Prefix Trie)
type TrieNode struct {
	prefix   []byte
	isEnd    bool
	children map[byte]*TrieNode
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

func (r *RadixTree) Insert(id []byte) {
	node := r.root
	i := 0

	for i < len(id) {
		key := id[i]
		node, childExists := node.children[key] //check me

		if !childExists {
			newNode := TrieNode{prefix: id[i:], isEnd: true}
			node.children[key] = &newNode
			return
		}

		prefixLen := r.ComparePrefixes(id[i:], node.prefix)

		i += prefixLen
		if prefixLen < len(node.prefix) {
			newChild := TrieNode{prefix: node.prefix[prefixLen:]}
			newChild.isEnd = node.isEnd
			newChild.children = node.children
			node.prefix = node.prefix[:prefixLen]
			node.children[newChild.prefix[0]] = &newChild
			node.isEnd = i == len(id)
		}
	}
}
