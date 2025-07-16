package main

import (
	"fmt"
	"math/rand"
	"time"
)

func main() {
	c1 := make(chan bool)
	c2 := make(chan bool)

	go func() {
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
		c1 <- true
	}()

	go func() {
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
		c2 <- true
	}()

	select {
	case <-c1:
		fmt.Println("c1 won")
	case <-c2:
		fmt.Println("c2 won")
	}
}
