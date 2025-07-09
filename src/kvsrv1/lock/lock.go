package lock

import (
	"fmt"
	"log"
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

// Type Lock represents a lock layered on the client Clerk.Put() and Clerk.Get()
// operations. Lock supports two methods: Acquire and Release.
// Only one client may successfully acquire the lock at a time; other clients
// must wait until the first client has release the lock using Release.
type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// The specific key to store the "lock state"
	key string
	// clientID represents the lock state; empty if unlocked, otherwise a randomly generated string per lock client.
	clientID string
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{
		ck:       ck,
		key:      l,
		clientID: kvtest.RandValue(8),
	}
	return lk
}

// Acquire is a method on the Lock type that attempts to acquire a lock
// layerd on a client's Put and Get calls.
// The Get and Put calls act as an atomic test on the lock state,
// and determine if we can acquire the lock.
func (lk *Lock) Acquire() {
	// The question then becomes *how* we want to implement the lock.
	// It could be a simple spin-lock, with a sleep mechanism? -> No.
	// Let's try a simple spin-lock.
	// for { if sync.CompareAndSwap(?????, ????) { return } time.Sleep(???)}
	// What values are we swapping?
	//
	// Above is wrong; The IDEA IS correct, but we're not implementing an operating
	// system primitive.
	// Instead, we are testing against the LOCK STATE in this instance
	// but the idea is actually quite similar.
	// Our Clerk.Get() call is our atomic TAS/CAS instruction in this case,
	// and we are testing against it. Besides that, this is, in essence still a spin-lock.
	for {
		val, vers, err := lk.ck.Get(lk.key)
		switch err {
		case rpc.ErrNoKey:
			// Attempt to create a lock
			fmt.Printf("RPC NO KEY\n")
			rpcErr := lk.ck.Put(lk.key, lk.clientID, vers)
			if rpcErr == rpc.OK {
				return
			}
			// put failed, keep retrying
			fmt.Printf("ErrNoKey: Put failed, assuming someone got here first...\n")
			fmt.Printf("ErrNoKey: Put err: %v\n", rpcErr)
		case rpc.OK:
			// check if key is free on this key
			// if succeed, return
			if val == "" {
				// No one holds this lock if val == ""
				putErr := lk.ck.Put(lk.key, lk.clientID, vers)
				if putErr == rpc.OK {
					return
				}
				// Did the put to claim the lock actually succeed? (Maybe? Maybe not?)
				if putErr == rpc.ErrMaybe {
					// Check the truth of this by grabbing the lock
					val, _, getErr := lk.ck.Get(lk.key)
					if val == lk.clientID && getErr == rpc.OK {
						return
					}
					// otherwise continue
				}
			}
		case rpc.ErrMaybe:
			fmt.Printf("ErrMaybe Retry Get...\n")
		}
		// NOTE: Perhaps sleep here to save CPU cycles.
		time.Sleep(100 * time.Millisecond)
	}
}

// Release is a method on the Lock type that should release a lock Acquire()'d
// previously on this client.
func (lk *Lock) Release() {
	// As with Acquire(), this is doing simply the opposite of what the OS
	// spin-lock should do.
	val, vers, err := lk.ck.Get(lk.key)
	if err != rpc.OK {
		return
	}

	if val != lk.clientID {
		// someone is trying to release a lock they don't own
		log.Fatal(fmt.Sprintf("SOMEONE TRIED TO RELEASE LOCK DON'T OWN: %#v %#v\n", val, lk.clientID))
	}

	err = lk.ck.Put(lk.key, "", vers)
	if err != rpc.OK {
		return
	}
}
