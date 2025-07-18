// A simple implementation of a key/value server using RPC.
package main

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"sync"
)

// NOTE: Go follows an At-most-once procedure call RPC system.

//
// Common RPC request/reply definitions
//

// NOTE: Declare RPC-args for marshaling/unmarshaling as structs
const (
	OK       = "OK"
	ErrNoKey = "ErrNoKey"
)

type Err string

type PutArgs struct {
	Key   string
	Value string
}

type PutReply struct {
	Err Err
}

type GetArgs struct {
	Key string
}

type GetReply struct {
	Err   Err
	Value string
}

//
// NOTE: Client
//

func connect() *rpc.Client {
	client, err := rpc.Dial("tcp", ":1234")
	if err != nil {
		log.Fatal("dialing:", err)
	}
	return client
}

func get(key string) string {
	client := connect()
	// Fill in args
	args := GetArgs{"subject"}
	// Alloc response
	reply := GetReply{}
	// Generic stub to fill in method called, args, and reply/response
	// Internally, Call marshals the args, send msg to server over connection, and awaits response,
	// then filling in the reply structure by the call stub and then return out of the call.
	err := client.Call("KV.Get", &args, &reply)
	if err != nil {
		log.Fatal("error:", err)
	}
	client.Close()
	return reply.Value
}

func put(key string, val string) {
	client := connect()
	args := PutArgs{"subject", "6.824"}
	reply := PutReply{}
	err := client.Call("KV.Put", &args, &reply)
	if err != nil {
		log.Fatal("error:", err)
	}
	client.Close()
}

//
// NOTE: Server
//

type KV struct {
	mu sync.Mutex
	// Where we do the put/get operations on.
	data map[string]string
}

func server() {
	// Server preamble to setup
	kv := new(KV)
	kv.data = map[string]string{}
	// key operation; register all methods impl'd on the KV type with the RPC server.
	// NOTE: Only public methods are registered.
	rpcs := rpc.NewServer()
	rpcs.Register(kv)
	l, e := net.Listen("tcp", ":1234")
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go func() {
		for {
			conn, err := l.Accept()
			if err == nil {
				go rpcs.ServeConn(conn)
			} else {
				break
			}
		}
		l.Close()
	}()
}

func (kv *KV) Get(args *GetArgs, reply *GetReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	val, ok := kv.data[args.Key]
	if ok {
		reply.Err = OK
		reply.Value = val
	} else {
		reply.Err = ErrNoKey
		reply.Value = ""
	}
	return nil
}

func (kv *KV) Put(args *PutArgs, reply *PutReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.data[args.Key] = args.Value
	reply.Err = OK
	return nil
}

func main() {
	server()

	put("subject", "6.824")
	fmt.Printf("Put(subject, 6.824) done\n")
	fmt.Printf("get(subject) -> %s\n", get("subject"))
}
