package main

import (
	"fmt"
	"sync"
)

func main() {
	var wg sync.WaitGroup

	wg.Add(1)
	go func() { // This function call runs asynchronously
		defer wg.Done()
		fmt.Println("hello, world!")
	}()

	wg.Wait() // We need to wait for it to finish in main or it won't print
	fmt.Println("Hi from main")
}

func main() {
	x := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20}
	result := processConcurrently(x)
	fmt.Println("Original: ", x)
	fmt.Println("Processed: ", result)
}

func process(val int) int {
	// do something with val
	return val + 1
}

func processConcurrently(inVals []int) []int {
	// create channels for communication
	in := make(chan int, 5)
	out := make(chan int, 5)
	// launch goroutines
	for i := 0; i < 5; i++ {
		go func() {
			for val := range in {
				// read data from in
				out <- process(val)
			}
		}()
	}
	// load data from inVals into in channel
	go func() {
		for _, v := range inVals {
			in <- v
		}
		close(in)
	}()
	// read data from out channel
	outVals := make([]int, 0, len(inVals))
	for range inVals {
		outVals = append(outVals, <-out)
	}
	// return data
	return outVals
}
