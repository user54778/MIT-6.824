package main

import "fmt"

func fibonacci(n int, ch chan int) {
	x, y := 0, 1
	for i := 0; i < n; i++ {
		ch <- x
		// Tuple assignment
		// Evaluate rh-side FIRST, then vals assigned to lh-side
		x, y = y, x+y // x assigned orig val y, y is assigned sum orig x + y vals
	}
	// No longer sending values on ch
	close(ch)
}

func main() {
	ch := make(chan int, 10)
	go fibonacci(cap(ch), ch)
	// Receive values from channel ch until its closed
	for v := range ch {
		fmt.Println(v)
	}
	v, ok := <-ch
	fmt.Printf("Is ch closed? %v %v\n", v, ok)
}
