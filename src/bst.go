package main

import (
	"fmt"
)

// Node represents a node in the binary search tree
type Node struct {
	value int
	left  *Node
	right *Node
}

// BST represents the binary search tree
type BST struct {
	root *Node
}

// Insert adds a value to the BST
func (bst *BST) Insert(value int) {
	bst.root = insertNode(bst.root, value)
}

func insertNode(node *Node, value int) *Node {
	if node == nil {
		return &Node{value: value}
	}
	if value < node.value {
		node.left = insertNode(node.left, value)
	} else {
		node.right = insertNode(node.right, value)
	}
	return node
}

// Search finds a value in the BST
func (bst *BST) Search(value int) bool {
	return searchNode(bst.root, value)
}

func searchNode(node *Node, value int) bool {
	if node == nil {
		return false
	}
	if value == node.value {
		return true
	}
	if value < node.value {
		return searchNode(node.left, value)
	}
	return searchNode(node.right, value)
}

// InOrderTraversal traverses the BST in order
func (bst *BST) InOrderTraversal() {
	inOrderTraversal(bst.root)
}

func inOrderTraversal(node *Node) {
	if node != nil {
		inOrderTraversal(node.left)
		fmt.Print(node.value, " ")
		inOrderTraversal(node.right)
	}
}

// GenerateHash generates a hash for the BST
func (bst *BST) GenerateHash() int {
	hash := 1
	var values []int
	collectValues(bst.root, &values)
	for _, value := range values {
		newValue := value + 2
		hash = (hash*newValue + newValue) % 1000
	}
	return hash
}

func collectValues(node *Node, values *[]int) {
	if node != nil {
		collectValues(node.left, values)
		*values = append(*values, node.value)
		collectValues(node.right, values)
	}
}

func areIdentical(a, b *Node) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return a.value == b.value && areIdentical(a.left, b.left) && areIdentical(a.right, b.right)
}

// CompareTrees compares two BSTs for equality
// checks if two BSTs contain the same values
func CompareTrees(a, b *BST) bool {
	var valuesA, valuesB []int
	collectValues(a.root, &valuesA)
	collectValues(b.root, &valuesB)
	if len(valuesA) != len(valuesB) {
		return false
	}
	for i := range valuesA {
		if valuesA[i] != valuesB[i] {
			return false
		}
	}
	return true
}
