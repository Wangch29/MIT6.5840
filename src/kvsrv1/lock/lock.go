package lock

import (
	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	// "fmt"
	"strconv"
	"sync"
	"time"
)

var global_id int = 1
var id_mutex sync.Mutex

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck        kvtest.IKVClerk
	lock_name string
	id        string
	hold      bool
	version   rpc.Tversion
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	id_mutex.Lock()
	defer id_mutex.Unlock()
	lk := &Lock{ck: ck, lock_name: l, id: strconv.Itoa(global_id), hold: false, version: 0}
	global_id++
	return lk
}

func (lk *Lock) Acquire() {
	for {
		value, version, err := lk.ck.Get(lk.lock_name)

		if err == rpc.ErrNoKey {
			ok := lk.ck.Put(lk.lock_name, lk.id, 0)
			if ok == rpc.OK {
				lk.hold = true
				lk.version = 1
				return
			} else if ok == rpc.ErrMaybe {
				for {
					v, ver, e := lk.ck.Get(lk.lock_name)
					if e == rpc.OK && v == lk.id {
						lk.hold = true
						lk.version = ver
						return
					}
					break
				}
			}
		} else if err == rpc.OK && value == "" {
			ok := lk.ck.Put(lk.lock_name, lk.id, version)
			if ok == rpc.OK {
				lk.hold = true
				lk.version = version + 1
				return
			} else if ok == rpc.ErrMaybe {
				for {
					v, ver, e := lk.ck.Get(lk.lock_name)
					if e == rpc.OK && v == lk.id {
						lk.hold = true
						lk.version = ver
						return
					}
					break
				}
			}
		}

		// fmt.Printf("Client %s failed to acquire lock, retrying...\n", lk.id)
		time.Sleep(10 * time.Millisecond)
	}
}

func (lk *Lock) Release() {
	for {
		ok := lk.ck.Put(lk.lock_name, "", lk.version)
		if ok == rpc.OK {
			lk.hold = false
			return
		} else if ok == rpc.ErrMaybe {
			v, _, e := lk.ck.Get(lk.lock_name)
			if e == rpc.OK && v == "" {
				lk.hold = false
				return
			}
		}

		// fmt.Printf("Client %s failed to release lock, err: %s, retrying...\n", lk.id, ok)
		time.Sleep(10 * time.Millisecond)
	}
}
