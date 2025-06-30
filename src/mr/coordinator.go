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

// Type Task represents an aggregate of a State and an associated
// time a worker is taking to do a Task.
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

	mapCounter    int
	reduceCounter int

	finishMap    bool
	finishReduce bool

	exitedMapWorkers int
}

// Create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		inFiles:          files,
		nReduce:          nReduce,
		mapTasks:         make([]Task, len(files)),
		reduceTasks:      make([]Task, nReduce),
		mapCounter:       0,
		reduceCounter:    0,
		finishMap:        false,
		finishReduce:     false,
		exitedMapWorkers: 0,
		mu:               sync.Mutex{},
	}

	for m := range c.mapTasks {
		c.mapTasks[m].State = Idle
	}

	for r := range c.reduceTasks {
		c.reduceTasks[r].State = Idle
	}
	fmt.Printf("DEBUG: %d files versus %v\n", len(files), files)

	c.server()
	return &c
}

// RequestTask is a method on the Coordinator object that gives an RPC service to a worker client
// to request for work to do in the MapReduce framework. This method also has the responsibility
// for determining worker timeouts and when to hand out Map versus Reduce tasks, as well
// as when to shut down the RPC service.
func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.mapCounter < len(c.mapTasks) {
		fmt.Printf("DEBUG: Map counter: %d\n", c.mapCounter)
		c.mapTasks[c.mapCounter].StartTime = time.Now()
		c.mapTasks[c.mapCounter].State = InProgress

		reply.TaskType = Map
		reply.MapInput = c.inFiles[c.mapCounter]
		reply.TaskID = c.mapCounter
		reply.NReduceFiles = c.nReduce

		c.mapCounter++
		return nil
	}

	// Wait for Map workers to finish
	if !c.finishMap {
		fmt.Printf("c.mapCounter and mapTasks: %d %d\n", c.mapCounter, len(c.mapTasks))
		for i, task := range c.mapTasks {
			fmt.Printf("DEBUG: Map worker %d\n", i)
			if task.State != Completed {
				if time.Since(task.StartTime) > 10*time.Second {
					fmt.Printf("Map worker %d timed out in state %v; reassigning work\n", i, task)
					c.mapTasks[i].StartTime = time.Now()
					c.mapTasks[i].State = Idle
					c.exitedMapWorkers++
					continue

					/*
						reply.TaskType = Map
						reply.MapInput = c.inFiles[i]
						reply.TaskID = i
						reply.NReduceFiles = c.nReduce
						return nil
					*/
				} else if task.State == Idle {
					fmt.Printf("Reassigning idle %d task %v\n", i, task)
					reply.TaskType = Map
					reply.MapInput = c.inFiles[i]
					reply.TaskID = i
					reply.NReduceFiles = c.nReduce
					return nil
				} else {
					reply.TaskType = Wait
					return nil
				}
			}
		}
		c.finishMap = true
	}

	if c.exitedMapWorkers > 0 {
		fmt.Printf("Only reduce %d workers\n", len(c.mapTasks)-c.exitedMapWorkers)
		/*
			c.reduceTasks[c.reduceCounter].StartTime = time.Now()
			c.reduceTasks[c.reduceCounter].State = InProgress

			reply.TaskType = Reduce
			reply.TaskID = c.reduceCounter
			reply.NReduceFiles = c.nReduce
			reply.NMapTasks = len(c.inFiles)
			c.reduceCounter++
		*/
	} else if c.reduceCounter < c.nReduce {
		fmt.Printf("DEBUG: Reduce counter: %d\n", c.reduceCounter)
		c.reduceTasks[c.reduceCounter].StartTime = time.Now()
		c.reduceTasks[c.reduceCounter].State = InProgress

		reply.TaskType = Reduce
		reply.TaskID = c.reduceCounter
		reply.NReduceFiles = c.nReduce
		reply.NMapTasks = len(c.inFiles)
		c.reduceCounter++
		return nil
	}

	// Wait for Reduce workers to finish
	if !c.finishReduce {
		for i, task := range c.reduceTasks {
			if task.State != Completed {
				if time.Since(task.StartTime) > 10*time.Second {
					fmt.Printf("Reduce worker %d timed out in state %v; reassigning work\n", i, task)
					c.reduceTasks[i].StartTime = time.Now()
					c.reduceTasks[i].State = Idle
					continue
					/*
						reply.TaskType = Reduce
						reply.TaskID = i
						reply.NReduceFiles = c.nReduce
						return nil
					*/
				} else if task.State == Idle {
					fmt.Printf("Reassigning idle %d task %v\n", i, task)
					reply.TaskType = Reduce
					reply.TaskID = i
					reply.NReduceFiles = c.nReduce
					return nil
				} else {
					reply.TaskType = Wait
					return nil
				}
			}
		}
		c.finishReduce = true
	}

	// exit
	reply.TaskType = Exit
	return nil
}

// TaskComplete is a method on the Coordinator object that provides an RPC service to a worker client
// that tells the server it has completed its work task.
func (c *Coordinator) TaskComplete(args *TaskCompleteArgs, reply *TaskCompleteArgs) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	fmt.Printf("TaskComplete called by %v\n", args)
	// Grab args
	// Update this worker's task state
	// For now, simply mark as Completed?
	switch args.TaskType {
	case Map:
		c.mapTasks[args.TaskID].State = args.ClientState
	case Reduce:
		c.reduceTasks[args.TaskID].State = args.ClientState
	}

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
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.finishMap && c.finishReduce
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
