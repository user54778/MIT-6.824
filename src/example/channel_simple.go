package main

import "fmt"

func sum(s []int, ch chan int) {
	sum := 0
	for _, v := range s {
		sum += v
	}
	ch <- sum
}

func main() {
	s := []int{7, 2, 8, -9, 4, 0}

	// How goroutines communicate are channels
	// Reference type; zero value is nil
	// You are passing a POINTER to the channel
	ch := make(chan int)
	go sum(s[:len(s)/2], ch)
	go sum(s[len(s)/2:], ch)

	x, y := <-ch, <-ch

	// Closing a channel here is NOT required since we're
	// not waiting for a channel to close

	fmt.Println(x, y, x+y)
}

// Reading, Writing, and Buffering
// <- interacts with a channel
// a := <- ch // read val from ch and place in a
// ch <- b    // write val b into ch
// NOTE: Values written to a channel are read ONCE ONLY
//
// NOTE: Check zero value with comma ok to see if channel is still open
// v, ok := <-ch
