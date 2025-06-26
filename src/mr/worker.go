package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
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
		break
	}
}

// TODO: Implement me!
func mapWorker() {
	/*
		intermediate := []mr.KeyValue{}
		for _, filename := range os.Args[2:] {
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("cannot open %v", filename)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", filename)
			}
			file.Close()
			kva := mapf(filename, string(content))
			intermediate = append(intermediate, kva...)
		}
	*/
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

// ihash chooses the reduce task number for each
// KeyValue emitted by Map.
// use ihash(key) % NReduce
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func writeIntermediateFile(path string, kvPairs []KeyValue) error {
	/*
		cwd, err := os.Getwd()
		if err != nil {
			return fmt.Errorf("getwd() failed", err)
		}
	*/
	// Inter k/v pairs should be in format mr-X-Y

	panic("unimplemented")
}

/*
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {
	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}
*/
