package kvsrv

import (
	"fmt"
	"log"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	tester "6.5840/tester1"
)

const (
	maxRetries = 100
)

// Clerk is a type that represents how the client will interact with the
// k/v server, which sends RPCs to the server.
// Clients can send two different RPCs to the server with Clerk;
// Put and Get.
type Clerk struct {
	clnt   *tester.Clnt
	server string

	rpcState map[string]int // K: Version, V: RPC calls
}

func MakeClerk(clnt *tester.Clnt, server string) kvtest.IKVClerk {
	ck := &Clerk{
		clnt:     clnt,
		server:   server,
		rpcState: make(map[string]int),
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
	//retries := 0
	//for retries < maxRetries {
	/*
		fmt.Printf("CLIENT: Calling get %d time\n", retries)
		ok := ck.clnt.Call(ck.server, "KVServer.Get", &args, &reply)
		if !ok {
			log.Fatal("Call failed in client Get")
		}
		if reply.Err == rpc.ErrNoKey {
			break
		} else if reply.Err == rpc.OK {
			break
		}
	*/
	//retries++
	//fmt.Printf("CLIENT: Retrying... %v\n", retries)
	// time.Sleep(1 * time.Second)
	//}
	//
	ok := ck.clnt.Call(ck.server, "KVServer.Get", &args, &reply)
	if !ok {
		log.Fatal("Call failed in client Get")
	}
	if reply.Err == rpc.ErrNoKey {
		fmt.Printf("DEBUG: Client Get no key.\n")
		return "", 0, rpc.ErrNoKey
	}
	fmt.Printf("Do my values make sense? %v %v %v\n", reply.Value, reply.Version, reply.Err)
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
	ok := ck.clnt.Call(ck.server, "KVServer.Put", &args, &reply)
	if !ok {
		log.Fatal(ok)
	}
	if _, ok := ck.rpcState[value]; !ok {
		ck.rpcState[value] = 0
	} else {
		ck.rpcState[value]++
	}
	switch reply.Err {
	case rpc.ErrVersion:
		if ck.rpcState[value] == 1 {
			return rpc.ErrVersion
		} else {
			fmt.Printf("CLIENT: Maybe?: %v\n", key)
			return rpc.ErrMaybe
		}
	case rpc.ErrNoKey:
		// fmt.Printf("DEBUG: In client put, I should retry my rpc call... (ErrNoKey)\n ")
		return rpc.ErrNoKey
	case rpc.OK:
		return rpc.OK
	}
	log.Fatal("Unexpected reply.Err")
	return rpc.OK
	/*
		if !ok {
			fmt.Printf("DEBUG: Client Put failed.\n")
			return rpc.ErrVersion
		}
		switch reply.Err {
		case rpc.ErrVersion:
			fmt.Printf("DEBUG: Client Put failed with ErrVersion\n")
			return rpc.ErrVersion
		case rpc.ErrNoKey:
			fmt.Printf("DEBUG: In client put, I should retry my rpc call... (ErrNoKey)\n ")
			return rpc.ErrNoKey
		}
	*/
	// fmt.Printf("FATAL: Offending put: (%#v %#v %#v)\n", key, value, version)
	//return rpc.OK
}
