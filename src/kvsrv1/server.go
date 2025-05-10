package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	"6.5840/tester1"
)

const Debug = false

type ValueVersion struct {
	value   string
	version rpc.Tversion
}

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu    sync.Mutex
	kvmap map[string]ValueVersion
}

func MakeKVServer() *KVServer {
	kv := &KVServer{
		kvmap: make(map[string]ValueVersion),
	}
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	val, ok := kv.kvmap[args.Key]
	if !ok {
		reply.Err = rpc.ErrNoKey
		return
	}

	reply.Value = val.value
	reply.Version = val.version
	reply.Err = rpc.OK
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	val, ok := kv.kvmap[args.Key]

	if !ok {
		if args.Version == 0 {
			// Add new kv.
			kv.kvmap[args.Key] = ValueVersion{value: args.Value, version: 1}
			reply.Err = rpc.OK
			return
		} else {
			reply.Err = rpc.ErrNoKey
			return
		}
	}

	if args.Version != val.version {
		reply.Err = rpc.ErrVersion
		return
	}

	// Update value.
	kv.kvmap[args.Key] = ValueVersion{args.Value, val.version + 1}
	reply.Err = rpc.OK
}

// You can ignore Kill() for this lab
func (kv *KVServer) Kill() {
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []tester.IService {
	kv := MakeKVServer()
	return []tester.IService{kv}
}
