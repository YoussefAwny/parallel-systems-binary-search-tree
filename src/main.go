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

func parallel_compare(bsts []*BST, hashMap map[int][]int, numWorkers int) map[int][]int {
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

	// Spawn worker threads
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go worker()
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
			parallel_hashing(bsts, *hashWorkers)
		}
	}

	if *compWorkers == 1 {
		duplicates := sequential_compare(bsts, hashMap)
		print_duplicates(duplicates)
		// end of if *compWorkers == 1
	} else if *compWorkers > 1 {
		duplicates := parallel_compare(bsts, hashMap, *compWorkers)
		print_duplicates(duplicates)
	}

}
