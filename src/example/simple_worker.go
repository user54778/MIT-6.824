package main

import (
	"fmt"
	"time"
)

func worker(done chan bool) {
	fmt.Print("working...")
	time.Sleep(time.Second)
	fmt.Println("done!")

	done <- true
}

func main() {
	done := make(chan bool, 1)
	// Start a worker goroutine, giving the channel
	// to notify on
	go worker(done)

	<-done
}
