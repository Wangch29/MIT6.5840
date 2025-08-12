package shardgrp

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
	tester "6.5840/tester1"
)

type ValueVersion struct {
	Value   string
	Version rpc.Tversion
}

type ClientPutResult struct {
	ReqId  int64 // Unique ID for the client
	Result any
}

type ShardState int

const (
	ShardStateUnknown ShardState = iota // Unknown state, should not be used
	ShardStateServing                   // Normal state, shard is serving
	ShardStateFrozen                    // Shard is frozen, no Get/Puts allowed
	ShardStateDeleted                   // Shard is deleted, no Get/Puts allowed
)

type ShardMeta struct {
	State ShardState    // Current state of the shard
	Num   shardcfg.Tnum // Configuration number for the shard
}

type KVServer struct {
	gid    tester.Tgid
	me     int
	dead   int32 // set by Kill()
	rsm    *rsm.RSM
	frozen bool // for testing purposes

	mu               sync.RWMutex
	shardKvMap       map[shardcfg.Tshid]map[string]ValueVersion // shardcfg.Tshid -> kvmap
	clientPutResults map[int64]ClientPutResult                  // clientId -> ClientPutReq
	shardStates      map[shardcfg.Tshid]ShardMeta               // shardId -> ShardMeta{ShardState, shardcfg.Tnum}
}

func (kv *KVServer) DoOp(req any) any {
	switch args := req.(type) {
	case *shardrpc.PutArgs:
		kv.mu.Lock()
		defer kv.mu.Unlock()

		if prev, ok := kv.clientPutResults[args.ClientId]; ok {
			if args.ReqId == prev.ReqId {
				return prev.Result
			} else if args.ReqId < prev.ReqId {
				return rpc.PutReply{Err: rpc.ErrOutdatedRequest}
			}
		}

		shardId := shardcfg.Key2Shard(args.Key)
		if kv.shardStates[shardId].Num > args.Num {
			// Configuration is not newer, ignore
			res := rpc.PutReply{Err: rpc.ErrVersion}
			kv.clientPutResults[args.ClientId] = ClientPutResult{
				ReqId:  args.ReqId,
				Result: res,
			}
			return res
		}

		if kv.shardStates[shardId].State != ShardStateServing {
			// Shard is not serving, return ErrWrongGroup
			res := rpc.PutReply{Err: rpc.ErrWrongGroup}
			kv.clientPutResults[args.ClientId] = ClientPutResult{
				ReqId:  args.ReqId,
				Result: res,
			}
			return res
		}

		shardKvMap := kv.shardKvMap[shardId]
		val, ok := shardKvMap[args.Key]

		// key does not exist.
		if !ok {
			if args.Version == 0 {
				// Key does not exist and version is 0, install the value
				shardKvMap[args.Key] = ValueVersion{Value: args.Value, Version: 1}
				res := rpc.PutReply{Err: rpc.OK}
				kv.clientPutResults[args.ClientId] = ClientPutResult{
					ReqId:  args.ReqId,
					Result: res,
				}
				return res
			} else {
				// Key does not exist and version is not 0, return ErrNoKey
				res := rpc.PutReply{Err: rpc.ErrNoKey}
				kv.clientPutResults[args.ClientId] = ClientPutResult{
					ReqId:  args.ReqId,
					Result: res,
				}
				return res
			}
		}

		// key exists.
		if args.Version != val.Version {
			// Version does not match, return ErrVersion
			res := rpc.PutReply{Err: rpc.ErrVersion}
			kv.clientPutResults[args.ClientId] = ClientPutResult{
				ReqId:  args.ReqId,
				Result: res,
			}
			return res
		}
		// Version matches, update the value and increment version
		shardKvMap[args.Key] = ValueVersion{Value: args.Value, Version: val.Version + 1}

		res := rpc.PutReply{Err: rpc.OK}
		kv.clientPutResults[args.ClientId] = ClientPutResult{
			ReqId:  args.ReqId,
			Result: res,
		}
		return res

	case shardrpc.PutArgs:
		return kv.DoOp(&args)

	case *shardrpc.GetArgs:
		kv.mu.RLock()
		defer kv.mu.RUnlock()

		shardId := shardcfg.Key2Shard(args.Key)
		if kv.shardStates[shardId].Num > args.Num {
			// Shard has a newer configuration, ignore this request
			return rpc.GetReply{Err: rpc.ErrWrongGroup}
		}

		if kv.shardStates[shardId].State != ShardStateServing {
			// Shard is not serving, return ErrWrongGroup
			return rpc.GetReply{Err: rpc.ErrWrongGroup}
		}

		shardKvmap := kv.shardKvMap[shardId]
		val, ok := shardKvmap[args.Key]
		if !ok {
			// Key does not exist, return ErrNoKey
			return rpc.GetReply{Err: rpc.ErrNoKey}
		}
		// Key exists, return the value and version
		return rpc.GetReply{Value: val.Value, Version: val.Version, Err: rpc.OK}

	case shardrpc.GetArgs:
		return kv.DoOp(&args)

	case *shardrpc.InstallShardArgs:
		kv.mu.Lock()
		defer kv.mu.Unlock()

		if kv.shardStates[args.Shard].Num > args.Num {
			// Shard already has a newer configuration; request is stale
			return shardrpc.InstallShardReply{Err: rpc.ErrVersion}
		}
		/* if kv.shardStates[args.Shard].Num == args.Num {
			return shardrpc.InstallShardReply{Err: rpc.OK}
		} */

		shardId := args.Shard
		kv.shardStates[shardId] = ShardMeta{ShardStateServing, args.Num}

		r := bytes.NewBuffer(args.State)
		d := labgob.NewDecoder(r)
		shardkvmap := make(map[string]ValueVersion)
		d.Decode(&shardkvmap)
		kv.shardKvMap[shardId] = shardkvmap

		return shardrpc.InstallShardReply{Err: rpc.OK}

	case shardrpc.InstallShardArgs:
		return kv.DoOp(&args)

	case *shardrpc.FreezeArgs:
		kv.mu.Lock()
		defer kv.mu.Unlock()

		if kv.shardStates[args.Shard].Num > args.Num {
			// Shard has a newer configuration, caller sent a stale Num
			return shardrpc.FreezeReply{Err: rpc.ErrVersion}
		}

		if _, ok := kv.shardStates[args.Shard]; !ok {
			// Shard must be existed before freezing
			return shardrpc.FreezeReply{Err: rpc.ErrWrongGroup}
		}

		shardId := args.Shard
		kv.shardStates[shardId] = ShardMeta{ShardStateFrozen, args.Num}

		w := new(bytes.Buffer)
		e := labgob.NewEncoder(w)
		e.Encode(kv.shardKvMap[shardId])

		return shardrpc.FreezeReply{
			State: w.Bytes(),
			Num:   kv.shardStates[shardId].Num,
			Err:   rpc.OK,
		}

	case shardrpc.FreezeArgs:
		return kv.DoOp(&args)

	case *shardrpc.DeleteShardArgs:
		kv.mu.Lock()
		defer kv.mu.Unlock()

		if kv.shardStates[args.Shard].Num > args.Num {
			// Shard has a newer configuration; request is stale
			return shardrpc.DeleteShardReply{Err: rpc.ErrVersion}
		}
		/* if kv.shardStates[args.Shard].Num == args.Num {
			return shardrpc.DeleteShardReply{Err: rpc.OK}
		} */

		if meta, ok := kv.shardStates[args.Shard]; !ok || meta.State == ShardStateServing {
			// Shard must be frozen before deletion
			return shardrpc.DeleteShardReply{Err: rpc.ErrWrongGroup}
		}

		shardId := args.Shard
		kv.shardStates[shardId] = ShardMeta{ShardStateDeleted, args.Num}
		delete(kv.shardKvMap, shardId)

		return shardrpc.DeleteShardReply{Err: rpc.OK}

	case shardrpc.DeleteShardArgs:
		return kv.DoOp(&args)

	default:
		// Unsupported operation, return an error or nil
		log.Printf("KVServer[%d] received unsupported operation type: %T", kv.me, args)
		panic("unsupported operation type")
	}
}

func (kv *KVServer) Snapshot() []byte {
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(len(kv.shardKvMap))
	for shardId, shardKvMap := range kv.shardKvMap {
		e.Encode(shardId)
		e.Encode(shardKvMap)
	}
	e.Encode(kv.shardStates)
	e.Encode(kv.clientPutResults)

	return w.Bytes()
}

func (kv *KVServer) Restore(data []byte) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.shardKvMap = make(map[shardcfg.Tshid]map[string]ValueVersion)

	if len(data) == 0 {
		kv.shardStates = make(map[shardcfg.Tshid]ShardMeta)
		kv.clientPutResults = make(map[int64]ClientPutResult)
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var n int
	d.Decode(&n)
	for i := 0; i < n; i++ {
		var kvmap map[string]ValueVersion
		var shardId shardcfg.Tshid
		d.Decode(&shardId)
		d.Decode(&kvmap)
		kv.shardKvMap[shardId] = kvmap
	}

	var shardStates map[shardcfg.Tshid]ShardMeta
	d.Decode(&shardStates)
	kv.shardStates = shardStates

	var putResults map[int64]ClientPutResult
	d.Decode(&putResults)
	kv.clientPutResults = putResults
}

func (kv *KVServer) Get(args *shardrpc.GetArgs, reply *rpc.GetReply) {
	if kv.killed() {
		reply.Err = rpc.ErrWrongLeader
		return
	}

	_, isLeader := kv.rsm.Raft().GetState()
	if !isLeader {
		reply.Err = rpc.ErrWrongLeader
		return
	}

	_, re := kv.rsm.Submit(args)
	if re == rpc.ErrWrongLeader || re == nil {
		reply.Err = rpc.ErrWrongLeader
		return
	}
	*reply = re.(rpc.GetReply)
}

func (kv *KVServer) Put(args *shardrpc.PutArgs, reply *rpc.PutReply) {
	if kv.killed() {
		reply.Err = rpc.ErrWrongLeader
		return
	}

	_, isLeader := kv.rsm.Raft().GetState()
	if !isLeader {
		reply.Err = rpc.ErrWrongLeader
		return
	}

	_, re := kv.rsm.Submit(args)
	if re == nil {
		reply.Err = rpc.ErrWrongLeader
		return
	}
	*reply = re.(rpc.PutReply)
}

// Freeze the specified shard (i.e., reject future Get/Puts for this
// shard) and return the key/values stored in that shard.
func (kv *KVServer) Freeze(args *shardrpc.FreezeArgs, reply *shardrpc.FreezeReply) {
	if kv.killed() {
		reply.Err = rpc.ErrWrongLeader
		return
	}

	_, isLeader := kv.rsm.Raft().GetState()
	if !isLeader {
		reply.Err = rpc.ErrWrongLeader
		return
	}

	_, re := kv.rsm.Submit(args)
	if re == nil {
		reply.Err = rpc.ErrWrongLeader
		return
	}
	*reply = re.(shardrpc.FreezeReply)
}

// Install the supplied state for the specified shard.
func (kv *KVServer) InstallShard(args *shardrpc.InstallShardArgs, reply *shardrpc.InstallShardReply) {
	if kv.killed() {
		reply.Err = rpc.ErrWrongLeader
		return
	}

	_, isLeader := kv.rsm.Raft().GetState()
	if !isLeader {
		reply.Err = rpc.ErrWrongLeader
		return
	}

	_, re := kv.rsm.Submit(args)
	if re == nil {
		reply.Err = rpc.ErrWrongLeader
		return
	}
	*reply = re.(shardrpc.InstallShardReply)
}

// Delete the specified shard.
func (kv *KVServer) Delete(args *shardrpc.DeleteShardArgs, reply *shardrpc.DeleteShardReply) {
	if kv.killed() {
		reply.Err = rpc.ErrWrongLeader
		return
	}

	_, isLeader := kv.rsm.Raft().GetState()
	if !isLeader {
		reply.Err = rpc.ErrWrongLeader
		return
	}

	_, re := kv.rsm.Submit(args)
	if re == nil {
		reply.Err = rpc.ErrWrongLeader
		return
	}
	*reply = re.(shardrpc.DeleteShardReply)
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// StartShardServerGrp starts a server for shardgrp `gid`.
//
// StartShardServerGrp() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartServerShardGrp(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []tester.IService {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(shardrpc.PutArgs{})
	labgob.Register(shardrpc.GetArgs{})
	labgob.Register(shardrpc.FreezeArgs{})
	labgob.Register(shardrpc.InstallShardArgs{})
	labgob.Register(shardrpc.DeleteShardArgs{})
	labgob.Register(rsm.Op{})

	kv := &KVServer{gid: gid, me: me}

	kv.shardKvMap = make(map[shardcfg.Tshid]map[string]ValueVersion)
	kv.shardStates = make(map[shardcfg.Tshid]ShardMeta)
	kv.clientPutResults = make(map[int64]ClientPutResult)
	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)

	// gid1 initialize all shards.
	if kv.gid == shardcfg.Gid1 {
		for i := 0; i < shardcfg.NShards; i++ {
			kv.shardStates[shardcfg.Tshid(i)] = ShardMeta{State: ShardStateServing, Num: 0}
			kv.shardKvMap[shardcfg.Tshid(i)] = make(map[string]ValueVersion)
		}
	} else {
		// For other groups, initialize shards as Unknown state
		// They will be set to Serving when they receive the configuration
		for i := 0; i < shardcfg.NShards; i++ {
			kv.shardStates[shardcfg.Tshid(i)] = ShardMeta{State: ShardStateUnknown, Num: 0}
			kv.shardKvMap[shardcfg.Tshid(i)] = make(map[string]ValueVersion)
		}
	}

	// Your code here
	return []tester.IService{kv, kv.rsm.Raft()}
}
