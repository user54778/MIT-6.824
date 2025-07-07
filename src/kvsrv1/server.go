package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

// ValueVersion is a tuple-like struct holding a value pair
// for a key entry in the server.
type valueVersion struct {
	value   string       // value user sends via the Clerk
	version rpc.Tversion // number of times a key has been written
}

// KVServer represents the server with which the Clerk representing a client
// node sends k/v pairs via RPC to. The KVServer should maintain
// an in-memory map that records for each key a (value, version tuple).
// The version number records the number of times the key has been written.
type KVServer struct {
	mu sync.Mutex // protect the server

	versionMap map[string]valueVersion
}

func MakeKVServer() *KVServer {
	kv := &KVServer{
		versionMap: make(map[string]valueVersion),
	}
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if v, ok := kv.versionMap[args.Key]; ok {
		reply.Value = v.value
		reply.Version = v.version
		// fmt.Printf("DEBUG: Server Get %v\n", v.value)
		reply.Err = rpc.OK
		return
	} else if !ok {
		reply.Err = rpc.ErrNoKey
		return
	}
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if v, ok := kv.versionMap[args.Key]; ok {
		// fmt.Printf("SRV: ok in put server; %v. Key: %v\n", v, args.Key)
		if v.version == args.Version {
			// fmt.Printf("SRV: %v matches in server\n", v.version)
			// NOTE: v is the old value w/ old version num. We want a new valueVersion
			// to be inserted in the map with this key
			newVersion := valueVersion{
				value:   args.Value,
				version: v.version + 1,
			}
			kv.versionMap[args.Key] = newVersion

			reply.Err = rpc.OK
			return
		} else {
			// fmt.Printf("SRV: versions %v %v dont match in server. Attempted: %v\n", v.version, args.Version, args.Key)
			reply.Err = rpc.ErrVersion
			return
		}
	} else if args.Version == 0 {
		// fmt.Printf("SRV: not ok in put server, but args.Version %v. Key attempted: %v\n", args.Version, args.Key)

		newVersion := valueVersion{
			value:   args.Value,
			version: 1,
		}
		kv.versionMap[args.Key] = newVersion
		// fmt.Printf("SRV: #%v\n", kv.versionMap[args.Key])

		reply.Err = rpc.OK
		return
	} else if !ok {
		reply.Err = rpc.ErrNoKey
		return
	}
}

// FIXME: You can ignore Kill() for this lab
func (kv *KVServer) Kill() {
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []tester.IService {
	kv := MakeKVServer()
	return []tester.IService{kv}
}
