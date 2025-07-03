package kvsrv

import (
	"fmt"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	tester "6.5840/tester1"
)

// Clerk is a type that represents how the client will interact with the
// k/v server, which sends RPCs to the server.
// Clients can send two different RPCs to the server with Clerk;
// Put and Get.
type Clerk struct {
	clnt   *tester.Clnt
	server string
}

func MakeClerk(clnt *tester.Clnt, server string) kvtest.IKVClerk {
	ck := &Clerk{clnt: clnt, server: server}
	// NOTE: You may add code here.
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
	ok := ck.clnt.Call(ck.server, "KVServer.Get", &args, &reply)
	if !ok {
		if reply.Err == rpc.ErrNoKey {
			return "", 0, rpc.ErrNoKey
		}
		fmt.Printf("DEBUG: In client get, I should infinitely loop here...\n")
	}
	switch reply.Err {
	case rpc.ErrNoKey:
		return "", 0, rpc.ErrNoKey
	default:
		fmt.Printf("What error did you fail with? %#v\n", reply.Err)
	}
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
	fmt.Printf("FATAL: Offending put: (%#v %#v %#v)\n", key, value, version)
	return rpc.OK
}
