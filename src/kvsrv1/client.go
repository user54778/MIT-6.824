package kvsrv

import (
	"fmt"
	"log"
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	tester "6.5840/tester1"
)

type retryReturn struct {
	success bool
	retries int
}

// Clerk is a type that represents how the client will interact with the
// k/v server, which sends RPCs to the server.
// Clients can send two different RPCs to the server with Clerk;
// Put and Get.
type Clerk struct {
	clnt   *tester.Clnt
	server string

	// rpcState int // K: Version, V: RPC calls
	retryReturn retryReturn
}

func MakeClerk(clnt *tester.Clnt, server string) kvtest.IKVClerk {
	ck := &Clerk{
		clnt:   clnt,
		server: server,
		retryReturn: retryReturn{
			success: false,
			retries: 0,
		},
	}
	return ck
}

// Get fetches the current value and version for a key.  It returns
// ErrNoKey if the key does not exist. It keeps trying forever in the
// face of all other errors.
//
// You can send an RPC with code like this:
// ok := ck.clnt.Call(ck.server, "KVServer.Get", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	args := rpc.GetArgs{
		Key: key,
	}
	reply := rpc.GetReply{}
	/*
		for {
			fmt.Printf("Calling ok in Get...\n")
			ok := ck.clnt.Call(ck.server, "KVServer.Get", &args, &reply)
			if !ok {
				// log.Fatal("Call failed in client Get")
				fmt.Printf("DEBUG: No response in Get. Retrying...\n")
				time.Sleep(100 * time.Millisecond)
			} else {
				fmt.Printf("Get response ok: %#v\n", reply)
				break
			}
		}
	*/
	retry := retryCall(func() bool {
		return ck.clnt.Call(ck.server, "KVServer.Get", &args, &reply)
	}, 100)
	if !retry.success {
		fmt.Printf("Fatal in Get?\n")
	}

	if reply.Err == rpc.ErrNoKey {
		fmt.Printf("DEBUG: Client Get no key.\n")
		return "", 0, rpc.ErrNoKey
	}
	// fmt.Printf("Do my values make sense? %v %v %v\n", reply.Value, reply.Version, reply.Err)
	return reply.Value, reply.Version, rpc.OK
}

// Put updates key with value only if the version in the
// request matches the version of the key at the server.  If the
// versions numbers don't match, the server should return
// ErrVersion.  If Put receives an ErrVersion on its first RPC, Put
// should return ErrVersion, since the Put was definitely not
// performed at the server. If the server returns ErrVersion on a
// resend RPC, then Put must return ErrMaybe to the application, since
// its earlier RPC might have been processed by the server successfully
// but the response was lost, and the Clerk doesn't know if
// the Put was performed or not.
//
// You can send an RPC with code like this:
// ok := ck.clnt.Call(ck.server, "KVServer.Put", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Put(key, value string, version rpc.Tversion) rpc.Err {
	args := rpc.PutArgs{
		Key:     key,
		Value:   value,
		Version: version,
	}
	reply := rpc.PutReply{}
	retry := retryCall(func() bool {
		return ck.clnt.Call(ck.server, "KVServer.Put", &args, &reply)
	}, 100)
	if !retry.success {
		fmt.Printf("Fatal?\n")
	}

	/*
		if !ok {
			// log.Fatal(ok)
			fmt.Printf("Retry count: %d\n", retries)
			fmt.Printf("DEBUG: No response in Put. Retrying...\n")
			time.Sleep(100 * time.Millisecond)
		} else {
			fmt.Printf("Put OK.\n")
		}
	*/

	/*
		if _, ok := ck.rpcState[value]; !ok {
			ck.rpcState[value] = 0
		} else {
			ck.rpcState[value]++
		}
	*/
	// fmt.Printf("DEBUG: rpcState: %#v\n", ck.rpcState)

	switch reply.Err {
	case rpc.ErrVersion:
		if retry.retries < 1 {
			// fmt.Printf("DEBUG: PUT ErrVersion\n")
			return rpc.ErrVersion
		} else {
			// fmt.Printf("DEBUG: Maybe?: %v\n", key)
			return rpc.ErrMaybe
		}
	case rpc.ErrNoKey:
		// fmt.Printf("DEBUG: In client put, I should retry my rpc call... (ErrNoKey)\n ")
		return rpc.ErrNoKey
	case rpc.OK:
		return rpc.OK
	}
	log.Fatal("Impossible reply.Err")
	return rpc.ErrVersion
}

// retryCall simply loops up to maxRetries times while calling closure
// fn. It is used for making an RPC call in the Clerk and retrying failed
// RPC's due to network issues.
func retryCall(fn func() bool, maxRetries int) retryReturn {
	for retries := 0; retries < maxRetries; retries++ {
		if ok := fn(); ok {
			return retryReturn{
				success: true,
				retries: retries,
			}
		}

		if retries < maxRetries-1 {
			time.Sleep(100 * time.Millisecond)
		}
	}

	return retryReturn{
		success: false,
		retries: maxRetries,
	}
}
