package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string,
) {
	// The worker will infinitely loop
	// Need to make a request/reply RPC
	// In our reply, we will get which task type to do
	// If Map/Reduce, we call mapWorker/reduceWorker respectively
	// Wait is a simple Sleep
	// Exit would simply call os.Exit()
	// Default could be same as Wait
	for {
		args := RequestTaskArgs{}
		reply := RequestTaskReply{}
		err := call("Coordinator.RequestTask", &args, &reply)
		if !err {
			break
		}
		switch reply.TaskType {
		case Map:
			mapWorker(mapf, &reply)
		case Reduce:
			reduceWorker(reducef, &reply)
		case Wait:
			time.Sleep(1 * time.Second)
		case Exit:
			os.Exit(0)
		default:
			time.Sleep(1 * time.Second)
		}
	}
}

func mapWorker(mapf func(string, string) []KeyValue, reply *RequestTaskReply) {
	// A) Read file (input split in paper)
	// C) Pass each k/v pair to mapf
	// E) What we actually do -> Write output of mapf to temp file, then final output
	// when done
	data, err := os.ReadFile(reply.MapInput)
	if err != nil {
		log.Fatalf("failed to read %v", reply.MapInput)
	}
	kva := mapf(reply.MapInput, string(data))
	// Divide intermediate keys into buckets for R reduce tasks.
	// R reduce tasks, R partitions
	// Partition 0 -> Reduce task 0
	// ...
	// Partition R - 1 -> Reduce task R - 1
	// Each partition can contain multiple kvPairs the Reduce task can process
	// We use ihash() % nReduce to place a reduce task
	partition := make([][]KeyValue, reply.NReduceFiles)
	for _, kv := range kva {
		bucket := ihash(kv.Key) % reply.NReduceFiles
		partition[bucket] = append(partition[bucket], kv)
	}
	// Tempwrite file
	// Atomic Rename
	for r, kvs := range partition {
		writeIntermediateFile(reply.TaskID, r, kvs)
	}
	// fmt.Printf("DEBUG: Map worker %d finished\n", reply.TaskID)

	// Update task state for server by calling the server
	updateRequest := TaskCompleteArgs{
		ClientState: Completed,
		TaskType:    Map,
		TaskID:      reply.TaskID,
	}
	updateReply := TaskCompleteArgs{}
	call("Coordinator.TaskComplete", &updateRequest, &updateReply)
}

func reduceWorker(reducef func(string, []string) string, reply *RequestTaskReply) error {
	// How do we perform shuffle?
	// We need to read in the file(s) to perform reduce on.
	kva := []KeyValue{}
	for mapTask := 0; mapTask < reply.NMapTasks; mapTask++ {
		s := fmt.Sprintf("mr-%d-%d", mapTask, reply.TaskID)
		f, err := os.Open(s)
		if err != nil {
			return errors.New(fmt.Sprintf("failed to open %v in reduceWorker", f, err))
		}
		dec := json.NewDecoder(f)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		f.Close()
	}
	// Shuffle: Sort by intermediate key such that occurrences of
	// the same key are grouped together
	sort.Slice(kva, func(i, j int) bool {
		return kva[i].Key < kva[j].Key
	})

	// Perform Reduce
	oname := fmt.Sprintf("mr-out-%d", reply.TaskID)
	cwd, err := os.Getwd()
	if err != nil {
		return fmt.Errorf("os.Getwd() failed in reduce")
	}
	ofile, err := os.CreateTemp(cwd, oname)
	if err != nil {
		return fmt.Errorf("os.CreateTemp() failed in reduce")
	}

	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

		i = j
	}
	err = os.Rename(ofile.Name(), oname)
	if err != nil {
		return fmt.Errorf("os.Rename() failed in reduce")
	}
	// fmt.Println("DEBUG: REDUCE WORKER DONE")
	// Send RPC request/response back to coordinator
	// Update task state for server by calling the server
	updateRequest := TaskCompleteArgs{
		ClientState: Completed,
		TaskType:    Reduce,
		TaskID:      reply.TaskID,
	}
	updateReply := TaskCompleteArgs{}
	call("Coordinator.TaskComplete", &updateRequest, &updateReply)
	return nil
}

// ihash chooses the reduce task number for each
// KeyValue emitted by Map.
// use ihash(key) % NReduce
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// writeIntermediateFile is a helper function to write intermediate k/v pairs
// from the Map worker to a temporary file, encode its data to json
// and then atomically store this temp file as a file on disk.
//
// The output format should look like mr-X-Y.
func writeIntermediateFile(x, y int, kvs []KeyValue) error {
	cwd, err := os.Getwd()
	if err != nil {
		return fmt.Errorf("getwd() failed")
	}
	tempName := fmt.Sprintf("mr-%d-%d", x, y)
	f, err := os.CreateTemp(cwd, tempName)
	if err != nil {
		return errors.New("failed to create temporary file in writeIntermediateFile")
	}
	enc := json.NewEncoder(f)
	for _, kv := range kvs {
		err = enc.Encode(&kv)
		if err != nil {
			return errors.New("failed to encode kv pair in writeIntermediateFile")
		}
	}
	err = os.Rename(f.Name(), tempName)
	if err != nil {
		return errors.New("failed to atomically rename temp file in writeIntermediateFile")
	}
	return nil
}

// Used in reduce
/*
func writeFinalOutput(reduce int) (error, []KeyValue) {
	cwd, err := os.Getwd()
	if err != nil {
		return fmt.Errorf("getwd() failed")
	}
	tempName := fmt.Sprintf("mr-out-%d",  reduce)
  f, err := os.CreateTemp(cwd, tempName)
  if err != nil {
    return errors.New("failed to create temp file in writeFinalOutput")
  }
  dec := json.NewDecoder(f)
  kva := make([]KeyValue, 0)
  for {
    var kv KeyValue
    if err := dec.Decode(&kv); err != nil {
      break
    }
    kva = append(kva, kv)
  }
	return nil
}
*/

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
