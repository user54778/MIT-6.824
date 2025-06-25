package main

import (
	"fmt"
	"math/rand"
	"sync/atomic"
	"time"
)

// State is owned by a single goroutine.
// This way we can guarantee data is never corrupted with concurrent access.
// To r/w that state, other goroutines will send msgs to the owning goroutine
// and receive corresponding replies.
// readOp and writeOp encapsulate those requests.

// readOp describes a read operation on a key
// in a map, and a channel to read that value.
type readOp struct {
	key  int
	resp chan int
}

// writeOp describes a k/v pair on the hashmap,
// with a response channel to indicate success upon writing
// the k/v pair.
type writeOp struct {
	key  int
	val  int
	resp chan bool
}

func main() {
	// How many operations we'll perform
	var readOps uint64
	var writeOps uint64

	// Used by other goroutines to issue r/w requests
	reads := make(chan readOp)
	writes := make(chan writeOp)

	// NOTE: Owning goroutine of state.
	go func() {
		// State is simply a map
		state := make(map[int]int)
		// Repeatedly select on r/w channels, responding to reqs
		// as they arrive.
		// A response is executed by performing the requested operation,
		// then sending a value on the response channel to indicate
		// success.
		for {
			select {
			case read := <-reads:
				read.resp <- state[read.key]
			case write := <-writes:
				state[write.key] = write.val
				write.resp <- true
			}
		}
	}()

	// 100 goroutines for issuing reads to the state-owning
	// goroutine via reads channel.
	for range 100 {
		go func() {
			for {
				// Construct a readOp to send
				read := readOp{
					key:  rand.Intn(5),
					resp: make(chan int),
				}
				reads <- read // send downstream
				<-read.resp   // receive upstream
				atomic.AddUint64(&readOps, 1)
				time.Sleep(time.Millisecond)
			}
		}()
	}

	// 10 goroutines for writing
	for range 10 {
		go func() {
			for {
				write := writeOp{
					key:  rand.Intn(5),
					val:  rand.Intn(100),
					resp: make(chan bool),
				}
				writes <- write
				<-write.resp
				atomic.AddUint64(&writeOps, 1)
				time.Sleep(time.Millisecond)
			}
		}()
	}

	time.Sleep(time.Second)

	readOpsFinal := atomic.LoadUint64(&readOps)
	fmt.Println("readOps:", readOpsFinal)
	writeOpsFinal := atomic.LoadUint64(&writeOps)
	fmt.Println("writeOps:", writeOpsFinal)
}
