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
	for id, group := range duplicates {
		if len(group) >= 1 {
			fmt.Printf("group %d: %d ", groupID, id)
			for _, gid := range group {
				fmt.Printf("%d ", gid)
			}
			fmt.Println()
			groupID++
		}
	}
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
			// Generate hashes and identify duplicates
			for i, bst := range bsts {
				hash := bst.GenerateHash()
				hashMap[hash] = append(hashMap[hash], i)
			}
			fmt.Printf("hashGroupTime: %v\n", 000)

			print_hash_grps(hashMap)
		}
	} else if *hashWorkers > 1 {
		// Add your logic here for when hashWorkers is greater than 1
		if *dataWorkers == 1 {
			// Generate hashes
			// hashChan := make(chan int, len(bsts))
			hashChan := make(chan [2]int, len(bsts))

			var wg sync.WaitGroup
			start := time.Now()

			for i, bst := range bsts {
				wg.Add(1)
				go func(i int, bst *BST, c chan [2]int) {
					c <- [2]int{i, bst.GenerateHash()}
					defer wg.Done()
				}(i, bst, hashChan)
			}
			wg.Wait()
			close(hashChan)

			// Identify trees with the same hash
			for pair := range hashChan {
				i, hash := pair[0], pair[1]
				hashMap[hash] = append(hashMap[hash], i)
			}

			hashGroupTime := time.Since(start).Seconds()
			fmt.Printf("hashGroupTime: %v\n", hashGroupTime)

			// Print hash groups
			print_hash_grps(hashMap)
		}
	}

	if *compWorkers == 1 {
		// Compare trees with identical hashes
		duplicates := make(map[int][]int)
		for _, ids := range hashMap {
			if len(ids) > 1 {
				for i := 0; i < len(ids); i++ {
					for j := i + 1; j < len(ids); j++ {
						if i == j {
							continue
						}
						if CompareTrees(bsts[ids[i]].root, bsts[ids[j]].root) {
							duplicates[ids[i]] = append(duplicates[ids[i]], ids[j])
						}
					}
				}
			}
		}

		fmt.Printf("compareTreeTime: %v\n", 000)
		print_duplicates(duplicates)

	} // end of if *compWorkers == 1

}
