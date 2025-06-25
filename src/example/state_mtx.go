package main

import (
	"fmt"
	"sync"
)

// Type Container holds a map of counters, with a mutex
// to synchronize access to it.
type Container struct {
	mu       sync.Mutex
	counters map[string]int
}

func (c *Container) inc(name string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.counters[name]++
}

func main() {
	// NOTE: Zero value of sync.Mutex is usable as-is; no need
	// to initialize
	c := Container{
		counters: map[string]int{"a": 0, "b": 0},
	}

	var wg sync.WaitGroup

	// Increment a named counter in a loop.
	doIncrement := func(name string, n int) {
		for range n {
			c.inc(name)
		}
		wg.Done()
	}

	wg.Add(3)
	// NOTE: All gorouties access the same Container object,
	// and two of three access the same counter.
	go doIncrement("a", 10000)
	go doIncrement("a", 10000)
	go doIncrement("b", 10000)

	wg.Wait()
	fmt.Println(c.counters)
}
