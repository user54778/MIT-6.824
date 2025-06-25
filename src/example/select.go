package main

import (
	"fmt"
	"time"
)

// This will deadlock; akin to locks needing to be acquired in a ordering,
// access to channels must be in the same order or they will deadlock.
/*
func main() {
	ch1 := make(chan int)
	ch2 := make(chan int)
	go func() {
		inGoroutine := 1
		ch1 <- inGoroutine
		fromMain := <-ch2
		fmt.Println("goroutine:", inGoroutine, fromMain)
	}()
	inMain := 2
  // NOTE: Inconsistent ordering
	ch2 <- inMain
	fromGoroutine := <-ch1
	fmt.Println("main:", inMain, fromGoroutine)
}
*/

// What is the solution to this? Using the select statement
// If we wrap the read/write to the channel in a select.
// The select will allow the goroutine to r/w from a set of multiple channels;
// random choice.
func main() {
	ch1 := make(chan int)
	ch2 := make(chan int)

	go func() {
		inGoroutine := 1
		ch1 <- inGoroutine
		fromMain := <-ch2
		// NOTE: This still will never launch - Waiting for a value to read from ch2
		fmt.Println("goroutine:", inGoroutine, fromMain)
	}()

	inMain := 2
	var fromGoroutine int

	select {
	// Write val inMain to ch2
	case ch2 <- inMain:
	// Write val ch1 into fromGoroutine
	case fromGoroutine = <-ch1:
	}

	fmt.Println("main:", inMain, fromGoroutine)
}

// Wait on multiple channels with for select.
func main() {
	// For our example we'll select across two channels.
	c1 := make(chan string)
	c2 := make(chan string)

	// Each channel will receive a value after some amount
	// of time, to simulate e.g. blocking RPC operations
	// executing in concurrent goroutines.
	go func() {
		time.Sleep(1 * time.Second)
		c1 <- "one"
	}()
	go func() {
		time.Sleep(2 * time.Second)
		c2 <- "two"
	}()

	// We'll use `select` to await both of these values
	// simultaneously, printing each one as it arrives.
	for range 2 {
		select {
		case msg1 := <-c1:
			fmt.Println("received", msg1)
		case msg2 := <-c2:
			fmt.Println("received", msg2)
		}
	}
	// Nothing to read off any channel here.
	// NOTE: The above will range twice; in a typical for range,
	// it loops forever until receiving a STOP signal of some sort.
}
