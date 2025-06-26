package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
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
		fmt.Printf("Response from server as task id: %d\n", reply.TaskID)
		switch reply.TaskType {
		case Map:
			mapWorker(mapf, &reply)
		case Reduce:
			reduceWorker()
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
	// Update task state for server by calling the server
	finishRequest := TaskCompleteArgs{
		ClientState: Completed,
	}
	finishResponse := TaskCompleteReply{}
	call("", &finishRequest, &finishResponse)
}

// TODO: Implement me!
func reduceWorker() {
	/*
					*
		*   Use sort.Slice() here instead
				sort.Sort(ByKey(intermediate))

				oname := "mr-out-0"
				ofile, _ := os.Create(oname)
						i := 0
						for i < len(intermediate) {
							j := i + 1
							for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
								j++
							}
							values := []string{}
							for k := i; k < j; k++ {
								values = append(values, intermediate[k].Value)
							}
							output := reducef(intermediate[i].Key, values)

							// this is the correct format for each line of Reduce output.
							fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

							i = j
						}
	*/
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
		return fmt.Errorf("getwd() failed", err)
	}
	// Inter k/v pairs should be in format mr-X-Y
	// enc := json.NewEncoder(file)
	// for _, kv := ... {
	//    err := enc.Encode(&kv)
	// }
	// os.CreateTemp -> os.Rename()
	//
	// TODO: implement me
	return nil
}

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
