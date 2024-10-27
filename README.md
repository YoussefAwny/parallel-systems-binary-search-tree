# Parallel Systems - Binary Search Tree

This repository contains an implementation of a parallel binary search tree (BST) as part of the course project for **Parallel Systems**. The focus of this project is on exploring parallelization techniques in managing and operating on a BST, particularly with a focus on improving performance in concurrent environments.

## Features
- **Parallel Operations**: The implementation supports parallel insertion, searching, and comparison of nodes in the BST.
- **Fine-Grained and Coarse-Grained Synchronization**: Different levels of synchronization are explored, providing insights into balancing efficiency and resource management.
- **Performance Analysis**: The code includes experiments comparing the performance of sequential and parallel operations, using different numbers of worker threads.

## Project Structure
- **`src/`**: Contains the main source code for the BST implementation.
- **`data/`**: Contains input files (`coarse.txt`, `fine.txt`, etc.) used for testing and performance analysis.
- **`docs/`**: Documentation and reports related to the project.
- **`README.md`**: This file.

## Getting Started

### Prerequisites
- **Go 1.16+**: The implementation is written in Go, and requires version 1.16 or higher.
- **Git**: For cloning the repository.

### Installation
1. Clone the repository:
   ```sh
   git clone https://github.com/YoussefAwny/parallel-systems-binary-search-tree.git
   ```
2. Navigate to the project directory:
   ```sh
   cd parallel-systems-binary-search-tree
   ```
3. Build the project:
   ```sh
   go build ./src
   ```

### Usage
- **Run the Program**:
  ```sh
  go run ./src/main.go
  ```
- **Configure Worker Threads**: Adjust the number of worker threads by modifying the configuration file or updating constants in the source code for different tests.

## Performance Evaluation
This project includes performance comparisons between sequential and parallel versions of BST operations:
- **`coarse.txt` and `fine.txt`** files: These are used to evaluate how different configurations of parallel workers impact the performance of the BST.
- Performance metrics such as `hashTime`, `compareTime`, and `hashGroupTime` are logged to help understand the effect of parallelization on speedup and efficiency.

## Contributing
Contributions are welcome! Feel free to open issues or submit pull requests.

1. Fork the repository.
2. Create a feature branch (`git checkout -b feature-branch`).
3. Commit your changes (`git commit -m 'Add a new feature'`).
4. Push to the branch (`git push origin feature-branch`).
5. Open a pull request.

## License
This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Contact
- **Youssef Awny** - [GitHub Profile](https://github.com/YoussefAwny)

Feel free to reach out if you have questions or suggestions for improvements!

