package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

// Type State is used to track the current state a worker is in.
type State int

const (
	Idle       State = iota // Task ready to be assigned to a worker
	InProgress              // Task assigned to worker
	Completed               // Successful completion of task
)

// String implements the Stringer interface for
// type State.
func (s State) String() string {
	switch s {
	case Idle:
		return "Idle"
	case InProgress:
		return "InProgress"
	case Completed:
		return "Completed"
	default:
		return "Unknown State"
	}
}

// TaskType represents a task a worker will have to complete.
type TaskType int

const (
	Map TaskType = iota
	Reduce
	Wait // No current work available; either due to map done being done, but reduce not ready,
	// or no work at all.
	Exit // The MapReduce system is complete; this worker should terminate
)

// String implements the stringer interface for TaskType.
func (s TaskType) String() string {
	switch s {
	case Map:
		return "Map task"
	case Reduce:
		return "Reduce task"
	case Wait:
		return "Wait task"
	case Exit:
		return "Exit task"
	default:
		return "Unknown TaskType"
	}
}

type Task struct {
	State     State
	StartTime time.Time
}

// Type Coordinator represents a special copy of the MapReduce program that assigns work
// to Workers. This work involves assigning files to workers as the task they need to perform.
// The Coordinator can be thought of as the conduit of where the location of intermediate files
// are propagated from Map workers to Reduce workers.
//
// The Coordinator will only select idle workers, assigning a Map or Reduce task.
// It is also the responsibility of the Coordinator to coordinate via RPC communication
// between Map and Reduce workers.
//
// To ensure fault tolerance, it is also the responsibility of the Coordinator to determine if
// a worker has faulted, where it must ping every worker on occasion and await for a response (10 seconds).
// If no response is received within this timeframe, assume the worker as dead.
//
// When the MapReduce job completes, the Coordinator is also responsible for
// waking up the caller user program with the results.
type Coordinator struct {
	mu      sync.Mutex
	inFiles []string
	nReduce int

	mapTasks    []Task
	reduceTasks []Task
}

// Create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		inFiles:     files,
		nReduce:     nReduce,
		mapTasks:    make([]Task, len(files)),
		reduceTasks: make([]Task, nReduce),
	}

	for m := range c.mapTasks {
		c.mapTasks[m].State = Idle
	}

	for r := range c.reduceTasks {
		c.reduceTasks[r].State = Idle
	}

	c.server()
	return &c
}

func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// More sophistication later for the reply;
	// for now we always reply with the Map task.
	// What data do I need to send over to the worker?
	// Indicate its a map task.
	// File name to process
	for i, task := range c.mapTasks {
		// Check for timed-out tasks
		if task.State == InProgress && time.Since(task.StartTime) > 10*time.Second {
			c.mapTasks[i].State = Idle
			c.mapTasks[i].StartTime = time.Time{}
		}

		// Assign tasks to idle workers
		if task.State == Idle {
			task.StartTime = time.Now()
			task.State = InProgress

			reply.TaskType = Map
			reply.MapInput = c.inFiles[i]
			reply.TaskID = i
			reply.NReduceFiles = c.nReduce
			return nil
		}
	}
	// maps done, assign reduce workers
	if c.allMapsDone() {
		// TODO : Implement me!
		fmt.Println("UNIMPLEMENTED")
	}
	// check everything done
	// shutdown
	return nil
}

// O(n)
func (c *Coordinator) allMapsDone() bool {
	for _, task := range c.mapTasks {
		if task.State != Completed {
			return false
		}
	}
	return true
}

func (c *Coordinator) allReducesDone() bool {
	for _, task := range c.reduceTasks {
		if task.State != Completed {
			return false
		}
	}
	return true
}

// Done
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	// TODO:
	return false
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	// l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}
