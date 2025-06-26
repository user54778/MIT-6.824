package main

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
)

// Provides access to EXPORTED methods of an object across a network.
//
// A) Server will register an object, and make that object visible as a service,
// with the name of the type of the object.
// Once registered, EXPORTED METHODS of that object can be accessed remotely.
// func (t *T) MethodName(argType T1, replyType *T2) error
// T1 represents the arguments provided by the caller
// T2 represents the result params to be returned to caller from callee
// The return value is passed back as a string the client can see
//
// A client uses the service by establishing a connection, then invoking NewClient on the connection.
// Dial performs both these steps for a raw connection.
// The Client object has two methods, Call and Go.

// Example: A server wants to export an object of type Arith

type Args struct {
	A int
	B int
}

type Quotient struct {
	Quo int
	Rem int
}

// Exported object
type Arith int

// Exported Method
func (t *Arith) Multiply(args *Args, reply *int) error {
	*reply = args.A * args.B
	return nil
}

// Exported Method
func (t *Arith) Divide(args *Args, quo *Quotient) error {
	if args.B == 0 {
		return errors.New("divide by zero")
	}
	quo.Quo = args.A / args.B
	quo.Rem = args.A % args.B
	return nil
}

func main() {
	// Register Arith as a method on the RPC
	arith := new(Arith)
	rpc.Register(arith)
	rpc.HandleHTTP()

	l, err := net.Listen("tcp", ":1234")
	if err != nil {
		log.Fatal(err)
	}
	go http.Serve(l, nil)

	// time.Sleep(3 * time.Second)

	// Clients should now see a Arith service with Arith.Multiply and Arith.Divide methods.
	client, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	if err != nil {
		log.Fatal(err)
	}
	args := &Args{7, 8}
	var reply int
	err = client.Call("Arith.Multiply", args, &reply)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Arith: %d * %d = %d\n", args.A, args.B, reply)
}
