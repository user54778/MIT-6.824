package mr

// RPC definitions.
import (
	"os"
	"strconv"
)

// Request: What does a worker send when asking for work?
// Reply: What does the coordinator send back?
// func (t *T) MethodName(argType T1, replyType *T2) error

// Type RequestTaskArgs represents a client request from a worker process.
type RequestTaskArgs struct{}

// Type RequestTaskReply describes an RPC response from the Coordinator server
// with metadata for a MapReduce task.
type RequestTaskReply struct {
	TaskType     TaskType
	MapInput     string // Input file for map task
	TaskID       int
	NReduceFiles int
}

type TaskCompleteArgs struct {
	ClientState State
}

type TaskCompleteReply struct{}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
