package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
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

func print_hash_grps(hashmap map[int][]int) {
	// Print hash groups
	for hash, ids := range hashmap {
		if len(ids) > 1 {
			fmt.Printf("%d: ", hash)
			for _, id := range ids {
				fmt.Printf("%d ", id)
			}
			fmt.Println()
		}
	}
}

func print_duplicates(duplicates map[int][]int) {
	// Print tree groups
	groupID := 0
	for _, group := range duplicates {
		uniqueGroup := make(map[int]bool)
		for _, gid := range group {
			if !uniqueGroup[gid] {
				uniqueGroup[gid] = true
			}
		}
		group = group[:0]
		for gid := range uniqueGroup {
			group = append(group, gid)
		}
		if len(group) > 1 {
			fmt.Printf("group %d: ", groupID)
			for _, gid := range group {
				fmt.Printf("%d ", gid)
			}
			fmt.Println()
			groupID++
		}
	}
}
func parallel_hashing(bsts []*BST, workerCount int) {
	// Generate hashes
	// index 0 is the index of the bst, index 1 is the hash value
	hashChan := make(chan [2]int, len(bsts))

	var wg sync.WaitGroup
	start_hash := time.Now()

	bst_per_worker := len(bsts) / workerCount
	bst_per_worker_remainder := len(bsts) % workerCount
	// fmt.Printf("bst_per_worker: %d, bst_per_worker_remainder: %d \n", bst_per_worker, bst_per_worker_remainder)
	// index 0 is the index of the first tree the worker will work on, index 1 is the number of BSTs to hash.
	jobs := make(chan [2]int, workerCount)

	// Start worker goroutines
	for w := 0; w < workerCount; w++ {
		wg.Add(1)
		go func(c chan [2]int, jobs chan [2]int) {
			defer wg.Done()
			for job := range jobs {
				for i := 0; i < job[1]; i++ {
					// fmt.Printf("bsts[%d]: \n", i+job[0])
					c <- [2]int{i + job[0], bsts[i+job[0]].GenerateHash()}
				}
			}

		}(hashChan, jobs)
	}

	// Send jobs to workers
	// Distribute jobs to workers
	startIndex := 0
	for w := 0; w < workerCount; w++ {
		numBSTs := bst_per_worker
		if w < bst_per_worker_remainder {
			numBSTs++
		}
		jobs <- [2]int{startIndex, numBSTs}
		startIndex += numBSTs
	}
	close(jobs)
	wg.Wait()
	close(hashChan)
	hashTime := time.Since(start_hash).Seconds()
	fmt.Printf("hashTime: %v\n", hashTime)
}

func parallel_hashing_routine_per_bst(bsts []*BST) {
	// Generate hashes
	hashChan := make(chan [2]int, len(bsts))

	var wg sync.WaitGroup
	start_hash := time.Now()

	// Start a goroutine for each BST
	for i, bst := range bsts {
		wg.Add(1)
		go func(index int, bst *BST) {
			defer wg.Done()
			hashChan <- [2]int{index, bst.GenerateHash()}
		}(i, bst)
	}

	wg.Wait()
	close(hashChan)
	hashTime := time.Since(start_hash).Seconds()
	fmt.Printf("hashTime: %v\n", hashTime)
}

func parallel_hashing_sequential_grouping(bsts []*BST, workerCount int) map[int][]int {
	hashMap := make(map[int][]int)
	// Generate hashes
	// index 0 is the index of the bst, index 1 is the hash value
	hashChan := make(chan [2]int, len(bsts))

	var wg sync.WaitGroup
	start_hash := time.Now()
	hash_group_time := time.Now()

	bst_per_worker := len(bsts) / workerCount
	bst_per_worker_remainder := len(bsts) % workerCount
	// fmt.Printf("bst_per_worker: %d, bst_per_worker_remainder: %d \n", bst_per_worker, bst_per_worker_remainder)
	// index 0 is the index of the first tree the worker will work on, index 1 is the number of BSTs to hash.
	jobs := make(chan [2]int, workerCount)

	// Start worker goroutines
	for w := 0; w < workerCount; w++ {
		wg.Add(1)
		go func(c chan [2]int, jobs chan [2]int) {
			defer wg.Done()
			job := <-jobs
			for i := range job[1] {
				// fmt.Printf("bsts[%d]: \n", i+job[0])
				c <- [2]int{i + job[0], bsts[i+job[0]].GenerateHash()}
			}

		}(hashChan, jobs)
	}

	// Send jobs to workers
	var last_index_used = 0
	for i := 0; i < workerCount; i++ {
		// fmt.Printf("bst_per_worker_remainder: %d,index to be used: %d", bst_per_worker_remainder, last_index_used)
		if bst_per_worker_remainder > 0 {
			jobs <- [2]int{last_index_used, bst_per_worker + 1}
			bst_per_worker_remainder--
			last_index_used = last_index_used + bst_per_worker + 1
			// fmt.Printf(" size: %d\n", bst_per_worker+1)
		} else {
			jobs <- [2]int{last_index_used, bst_per_worker}
			last_index_used = last_index_used + bst_per_worker
			// fmt.Printf(" size: %d\n", bst_per_worker)
		}

	}
	close(jobs)
	wg.Wait()
	close(hashChan)
	hashTime := time.Since(start_hash).Seconds()
	fmt.Printf("hashTime: %v\n", hashTime)

	// Identify trees with the same hash
	for pair := range hashChan {
		// fmt.Println(pair)
		i, hash := pair[0], pair[1]
		hashMap[hash] = append(hashMap[hash], i)
	}
	hashGroupTime := time.Since(hash_group_time).Seconds()
	fmt.Printf("hashGroupTime: %v\n", hashGroupTime)

	// Print hash groups
	print_hash_grps(hashMap)
	return hashMap
}

func parallel_hashing_parallel_grouping(bsts []*BST, workerCount int) map[int][]int {
	hashMap := make(map[int][]int)
	var mu sync.Mutex
	// Generate hashes
	// index 0 is the index of the bst, index 1 is the hash value

	var wg sync.WaitGroup
	start_hash := time.Now()

	bst_per_worker := len(bsts) / workerCount
	bst_per_worker_remainder := len(bsts) % workerCount
	// fmt.Printf("bst_per_worker: %d, bst_per_worker_remainder: %d \n", bst_per_worker, bst_per_worker_remainder)
	// index 0 is the index of the first tree the worker will work on, index 1 is the number of BSTs to hash.
	jobs := make(chan [2]int, workerCount)

	// Start worker goroutines
	for w := 0; w < workerCount; w++ {
		wg.Add(1)
		go func(jobs chan [2]int) {
			defer wg.Done()
			job := <-jobs
			for i := range job[1] {
				index := i + job[0]
				hash := bsts[index].GenerateHash()
				mu.Lock()
				hashMap[hash] = append(hashMap[hash], index)
				mu.Unlock()
			}

		}(jobs)
	}

	// Send jobs to workers
	var last_index_used = 0
	for i := 0; i < workerCount; i++ {
		// fmt.Printf("bst_per_worker_remainder: %d,index to be used: %d", bst_per_worker_remainder, last_index_used)
		if bst_per_worker_remainder > 0 {
			jobs <- [2]int{last_index_used, bst_per_worker + 1}
			bst_per_worker_remainder--
			last_index_used = last_index_used + bst_per_worker + 1
			// fmt.Printf(" size: %d\n", bst_per_worker+1)
		} else {
			jobs <- [2]int{last_index_used, bst_per_worker}
			last_index_used = last_index_used + bst_per_worker
			// fmt.Printf(" size: %d\n", bst_per_worker)
		}

	}
	close(jobs)
	wg.Wait()
	hashTime := time.Since(start_hash).Seconds()
	fmt.Printf("hashTime: %v\n", hashTime)
	fmt.Printf("hashGroupTime: %v\n", hashTime)

	// Print hash groups
	print_hash_grps(hashMap)
	return hashMap
}

func sequential_compare(bsts []*BST, hashMap map[int][]int) map[int][]int {
	// Compare trees with identical hashes
	start := time.Now()
	duplicates := make(map[int][]int)
	for hash, ids := range hashMap {
		if len(ids) > 1 {
			for i := 0; i < len(ids); i++ {
				for j := i + 1; j < len(ids); j++ {
					if i == j {
						continue
					}
					if CompareTrees(bsts[ids[i]], bsts[ids[j]]) {
						duplicates[hash] = append(duplicates[hash], ids[i])
						duplicates[hash] = append(duplicates[hash], ids[j])
					}
				}
			}
		}
	}
	compareTreeTime := time.Since(start).Seconds()
	fmt.Printf("compareTreeTime: %v\n", compareTreeTime)
	return duplicates
}

func parallel_compare(bsts []*BST, hashMap map[int][]int, numWorkers int, goroutine_per_comparison bool) map[int][]int {
	type work struct {
		hash, i, j int
	}

	start := time.Now()
	duplicates := make(map[int][]int)
	var mu sync.Mutex
	var wg sync.WaitGroup

	// Buffer to hold work
	buffer := make(chan work, 100)

	// Worker function
	worker := func() {
		defer wg.Done()
		for w := range buffer {
			// fmt.Printf("Worker %d: {%d %d}\n", id, w.i, w.j)
			if CompareTrees(bsts[w.i], bsts[w.j]) {
				mu.Lock()
				duplicates[w.hash] = append(duplicates[w.hash], w.i)
				duplicates[w.hash] = append(duplicates[w.hash], w.j)
				mu.Unlock()
			}
		}
	}
	if goroutine_per_comparison {
		// Spawn worker threads
		for i := 0; i < len(bsts); i++ {
			wg.Add(1)
			go worker()
		}
	} else {
		// Spawn worker threads
		for i := 0; i < numWorkers; i++ {
			wg.Add(1)
			go worker()
		}
	}

	// Generate work
	for hash, ids := range hashMap {
		if len(ids) > 1 {
			for i := 0; i < len(ids); i++ {
				for j := i + 1; j < len(ids); j++ {
					buffer <- work{hash: hash, i: ids[i], j: ids[j]}
				}
			}
		}
	}

	close(buffer)
	wg.Wait()

	compareTreeTime := time.Since(start).Seconds()
	fmt.Printf("compareTreeTime: %v\n", compareTreeTime)
	return duplicates
}

func main() {

	// Define command-line flags
	// number of hash workers to hash BSTs.
	hashWorkers := flag.Int("hash-workers", 0, "Number of hash worker threads")
	// number of workers to update the map.
	dataWorkers := flag.Int("data-workers", 0, "Number of data worker threads")
	// number of workers to do the comparisons.
	compWorkers := flag.Int("comp-workers", 0, "Number of comp worker threads")
	input := flag.String("input", "", "Path to the input file")
	goroutine_per_bst := flag.Bool("goroutine-per-bst", false, "spawning a goroutine for each BST to generate its own hash")
	goroutine_per_comparison := flag.Bool("goroutine-per-comparison", false, "spawning a goroutine for each comparison")

	// Parse the command-line flags
	flag.Parse()

	// Validate input file path
	if *input == "" {
		fmt.Println("Error: input file path is required")
		flag.Usage()
		os.Exit(1)
	}

	// Read the file
	content, err := os.ReadFile(*input)
	if err != nil {
		fmt.Printf("Error reading file: %v\n", err)
		os.Exit(1)
	}

	// Read the file line by line
	var bsts []*BST
	scanner := bufio.NewScanner(strings.NewReader(string(content)))
	for scanner.Scan() {
		line := scanner.Text()
		values := strings.Fields(line)
		bst := &BST{}
		for _, value := range values {
			num, err := strconv.Atoi(value)
			if err != nil {
				fmt.Printf("Error converting to integer: %v\n", err)
				os.Exit(1)
			}
			bst.Insert(num)
		}
		bsts = append(bsts, bst)
	}
	if err := scanner.Err(); err != nil {
		fmt.Printf("Error reading file: %v\n", err)
		os.Exit(1)
	}

	hashMap := make(map[int][]int)
	// -hash-workers=1 -data-workers=1
	if *hashWorkers == 1 {
		if *dataWorkers == 1 {
			start := time.Now()
			// Generate hashes and identify duplicates
			for i, bst := range bsts {
				hash := bst.GenerateHash()
				hashMap[hash] = append(hashMap[hash], i)
			}
			hashTime := time.Since(start).Seconds()
			fmt.Printf("hashTime: %v\n", hashTime)
			fmt.Printf("hashGroupTime: %v\n", hashTime)

			print_hash_grps(hashMap)
		} else if *dataWorkers < 1 {
			start := time.Now()
			// Generate hashes
			for _, bst := range bsts {
				bst.GenerateHash()
			}
			hashTime := time.Since(start).Seconds()
			fmt.Printf("hashTime: %v\n", hashTime)

		}
	} else if *hashWorkers > 1 {
		// Add your logic here for when hashWorkers is greater than 1
		if *dataWorkers == 1 {
			hashMap = parallel_hashing_sequential_grouping(bsts, *hashWorkers)
			// end of if *dataWorkers == 1
		} else if *dataWorkers > 1 {
			if *dataWorkers == *hashWorkers {
				hashMap = parallel_hashing_parallel_grouping(bsts, *hashWorkers)
			} else {
				fmt.Println("Error: data-workers must be equal to hash-workers")
			}
			// end of if *dataWorkers > 1
		} else {
			if *goroutine_per_bst {
				parallel_hashing_routine_per_bst(bsts)
			} else {
				parallel_hashing(bsts, *hashWorkers)
			}
		}
	}

	if *compWorkers == 1 {
		duplicates := sequential_compare(bsts, hashMap)
		print_duplicates(duplicates)
		// end of if *compWorkers == 1
	} else if *compWorkers > 1 {
		duplicates := parallel_compare(bsts, hashMap, *compWorkers, *goroutine_per_comparison)
		print_duplicates(duplicates)
	}

}
