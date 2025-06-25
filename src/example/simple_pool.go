package main

import (
	"fmt"
	"sync"
	"time"
)

// worker receives tasks on the jobs channel and sends the
// corresponding results to the results channel.
func worker(id int, jobs <-chan int, results chan<- int) {
	for j := range jobs {
		fmt.Println("worker", id, "started  job", j)
		time.Sleep(time.Second)
		fmt.Println("worker", id, "finished job", j)
		results <- j * 2
	}
}

func main() {
	// We need a way to coordinate and send out tasks for
	// our workers. We choose to use channels to accomplish
	// this task.
	const numJobs = 5
	jobs := make(chan int, numJobs)
	results := make(chan int, numJobs)

	// Spin up 3 workers; all of them immediately block in the worker
	// function since there are no jobs to process.
	for w := 1; w <= 3; w++ {
		go worker(w, jobs, results)
	}

	// Send numJobs to the jobs channel
	for j := 1; j <= numJobs; j++ {
		jobs <- j
	}
	// Close the channel to signal on this channel
	// that we are done sending jobs.
	close(jobs)

	// Collect results of workers.
	// NOTE: An alternative way to coordinate waiting
	// for workers to finish is with a WaitGroup
	for a := 1; a <= numJobs; a++ {
		<-results
	}
}

func worker(id int) {
	fmt.Printf("Worker %d starting\n", id)

	time.Sleep(time.Second)
	fmt.Printf("Worker %d done\n", id)
}

func main() {
	var wg sync.WaitGroup

	for i := 1; i <= 5; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()
			worker(i)
		}()
	}

	wg.Wait()
}
