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
	mu      sync.Mutex // Serialize access to Coordinator
	inFiles []string   // input files
	nReduce int        // reduce workers

	mapTasks    []Task // amount of mapTasks
	reduceTasks []Task // amount of reduceTasks

	mapCounter    int
	reduceCounter int

	// prevent over-calling allMapsDone and allReducesDone
	finishMap    bool
	finishReduce bool
}

// Create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		inFiles:       files,
		nReduce:       nReduce,
		mapTasks:      make([]Task, len(files)),
		reduceTasks:   make([]Task, nReduce),
		mapCounter:    0,
		reduceCounter: 0,
		finishMap:     false,
		finishReduce:  false,
		// exitedMapWorkers: 0,
		mu: sync.Mutex{},
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

	// NOTE: This acts as a rendezvous. It catches faults when a worker makes a request,
	// specifically in the event of failure.
	c.checkTimeouts()

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
				fmt.Printf("DEBUG: Map worker %d NOT complete\n", i)
				if task.State == InProgress && time.Since(task.StartTime) > 10*time.Second {
					fmt.Printf("Map worker %d timed out in state %v; reassigning work\n", i, task)
					c.mapTasks[i].StartTime = time.Now()
					c.mapTasks[i].State = Idle

					reply.TaskType = Map
					reply.MapInput = c.inFiles[i]
					reply.TaskID = i
					reply.NReduceFiles = c.nReduce
					return nil
				} else if task.State == Idle {
					fmt.Printf("Reassigning idle %d task %v\n", i, task)
					reply.TaskType = Map
					reply.MapInput = c.inFiles[i]
					reply.TaskID = i
					reply.NReduceFiles = c.nReduce
					return nil
				}
			}
		}
	}

	fmt.Println("Calling maps done")
	if c.allMapsDone() {
		c.finishMap = true
		fmt.Printf("DEBUG: All maps done\n")
	} else {
		fmt.Printf("Map is waiting...\n")
		reply.TaskType = Wait
		return nil
	}

	// Reduce tasks
	if c.reduceCounter < c.nReduce {
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
				if task.State == InProgress && time.Since(task.StartTime) > 10*time.Second {
					fmt.Printf("Reduce worker %d timed out in state %v; reassigning work\n", i, task)
					c.reduceTasks[i].StartTime = time.Now()
					c.reduceTasks[i].State = InProgress

					reply.TaskType = Reduce
					reply.TaskID = i
					reply.NReduceFiles = c.nReduce
					reply.NMapTasks = len(c.inFiles)
					return nil
				} else if task.State == Idle {
					fmt.Printf("Reassigning idle %d task %v\n", i, task)
					c.reduceTasks[i].StartTime = time.Now()
					c.reduceTasks[i].State = InProgress

					reply.TaskType = Reduce
					reply.TaskID = i
					reply.NReduceFiles = c.nReduce
					reply.NMapTasks = len(c.inFiles)
					return nil
				}
			}
		}
	}

	if c.allReducesDone() {
		c.finishReduce = true
		fmt.Printf("DEBUG: All reduce tasks done\n")
	} else {
		reply.TaskType = Wait
		return nil
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
	fmt.Printf("DEBUG: TaskComplete called by %v\n", args)
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

// checkTimeouts is a helper method used to check the edge case that one of the worker processes
// failed, but we aren't sure which. The problem is, we need this to act as a "rendezvous" point
// at the start of the RequestTask() method to reset any tasks that are timed out and reset
// their state.
func (c *Coordinator) checkTimeouts() {
	fmt.Println("DEBUG: In checkTimeouts")
	t := time.Now()

	for i, task := range c.mapTasks {
		if task.State != Completed {
			if time.Since(t) > 10*time.Second {
				fmt.Printf("%d is an Idle map task in checkTimeouts; resetting....\n", i)
				c.mapTasks[i].State = Idle
				c.mapTasks[i].StartTime = time.Time{}
			}
		}
	}

	for i, task := range c.reduceTasks {
		if task.State != Completed {
			if time.Since(t) > 10*time.Second {
				fmt.Printf("%d is an Idle reduce task in checkTimeouts; resetting....\n", i)
				c.reduceTasks[i].State = Idle
				c.reduceTasks[i].StartTime = time.Time{}
			}
		}
	}
}

func (c *Coordinator) allMapsDone() bool {
	// NOTE: Do NOT use a lock here since this is called from within RequestTask() which already
	// holds lock mu.
	for i, task := range c.mapTasks {
		if task.State != Completed {
			fmt.Printf("Task %d is not done in map\n", i)
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
