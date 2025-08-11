package shardgrp

import (
	"math/rand"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
	tester "6.5840/tester1"
)

type Clerk struct {
	clnt      *tester.Clnt
	servers   []string
	leaderIdx int // last successful leader (index into servers[])
	clerkId   int64
	reqId     int64
}

func MakeClerk(clnt *tester.Clnt, servers []string) *Clerk {
	ck := &Clerk{clnt: clnt, servers: servers}
	ck.servers = append([]string{}, servers...)
	ck.leaderIdx = 0
	ck.clerkId = int64(rand.Int63()) // Randomly generated client ID
	ck.reqId = 0
	return ck
}

func (ck *Clerk) Get(key string, n shardcfg.Tnum) (string, rpc.Tversion, rpc.Err) {
	retryCount := 0
	maxRetries := 10
	idx := ck.leaderIdx
	args := shardrpc.GetArgs{Key: key, Num: n}

	for {
		reply := rpc.GetReply{}

		ok := ck.clnt.Call(ck.servers[idx], "KVServer.Get", &args, &reply)

		if ok {
			if reply.Err == rpc.OK {
				ck.leaderIdx = idx
				return reply.Value, reply.Version, rpc.OK
			}
			if reply.Err == rpc.ErrNoKey {
				ck.leaderIdx = idx
				return "", 0, rpc.ErrNoKey
			}
			// Handle ErrWrongLeader by trying next server
			if reply.Err == rpc.ErrWrongLeader {
				idx = (idx + 1) % len(ck.servers)
				continue
			}
		}
		// RPC call failed, add delay before retrying
		if retryCount >= maxRetries {
			return "", 0, rpc.ErrWrongGroup
		}
		retryCount++
		time.Sleep(10 * time.Millisecond)
		idx = (idx + 1) % len(ck.servers)
	}
}

func (ck *Clerk) Put(key string, value string, version rpc.Tversion, n shardcfg.Tnum) (bool, rpc.Err) {
	retryCount := 0
	maxRetries := 10
	idx := ck.leaderIdx
	args := shardrpc.PutArgs{Key: key, Value: value, Version: version, Num: n, ClientId: ck.clerkId, ReqId: ck.reqId}
	ck.reqId++
	first := true

	for {
		reply := rpc.PutReply{}

		ok := ck.clnt.Call(ck.servers[idx], "KVServer.Put", &args, &reply)

		if ok {
			if reply.Err == rpc.OK {
				ck.leaderIdx = idx
				return true, rpc.OK
			}
			if reply.Err == rpc.ErrNoKey {
				ck.leaderIdx = idx
				return true, rpc.ErrNoKey
			}
			if reply.Err == rpc.ErrVersion {
				if first {
					ck.leaderIdx = idx
					return true, rpc.ErrVersion
				} else {
					return false, rpc.ErrMaybe
				}
			}
			if reply.Err == rpc.ErrOutdatedRequest {
				return true, rpc.ErrMaybe
			}
			// Handle ErrWrongLeader by trying next server
			if reply.Err == rpc.ErrWrongLeader {
				idx = (idx + 1) % len(ck.servers)
				continue
			}
		}
		// RPC call failed, add delay before retrying
		if retryCount >= maxRetries {
			return false, rpc.ErrWrongGroup
		}
		retryCount++
		time.Sleep(10 * time.Millisecond)
		first = false
		idx = (idx + 1) % len(ck.servers)
	}
}

func (ck *Clerk) Freeze(s shardcfg.Tshid, num shardcfg.Tnum) ([]byte, rpc.Err) {
	retryCount := 0
	maxRetries := 10
	idx := ck.leaderIdx
	args := shardrpc.FreezeArgs{Shard: s, Num: num}
	ck.reqId++

	for {
		reply := shardrpc.FreezeReply{}
		ok := ck.clnt.Call(ck.servers[idx], "KVServer.Freeze", &args, &reply)
		if ok {
			if reply.Err == rpc.OK {
				ck.leaderIdx = idx
				return reply.State, rpc.OK
			}
			if reply.Err == rpc.ErrVersion {
				ck.leaderIdx = idx
				return nil, rpc.ErrVersion
			}
			// Handle ErrWrongLeader by trying next server
			if reply.Err == rpc.ErrWrongLeader {
				idx = (idx + 1) % len(ck.servers)
				continue
			}
		}
		// RPC call failed, add delay before retrying
		if retryCount >= maxRetries {
			return []byte{}, rpc.ErrWrongGroup
		}
		retryCount++
		time.Sleep(10 * time.Millisecond)
		idx = (idx + 1) % len(ck.servers)
	}
}

func (ck *Clerk) InstallShard(s shardcfg.Tshid, state []byte, num shardcfg.Tnum) rpc.Err {
	retryCount := 0
	maxRetries := 10
	idx := ck.leaderIdx
	args := shardrpc.InstallShardArgs{Shard: s, State: state, Num: num}

	for {
		reply := shardrpc.InstallShardReply{}
		ok := ck.clnt.Call(ck.servers[idx], "KVServer.InstallShard", &args, &reply)
		if ok {
			if reply.Err == rpc.OK {
				ck.leaderIdx = idx
				return rpc.OK
			}
			if reply.Err == rpc.ErrVersion {
				ck.leaderIdx = idx
				return rpc.ErrVersion
			}
			// Handle ErrWrongLeader by trying next server
			if reply.Err == rpc.ErrWrongLeader {
				idx = (idx + 1) % len(ck.servers)
				continue
			}
		}
		// RPC call failed, add delay before retrying
		if retryCount >= maxRetries {
			return rpc.ErrWrongGroup
		}
		retryCount++
		time.Sleep(10 * time.Millisecond)
		idx = (idx + 1) % len(ck.servers)
	}
}

func (ck *Clerk) Delete(s shardcfg.Tshid, num shardcfg.Tnum) rpc.Err {
	retryCount := 0
	maxRetries := 10
	idx := ck.leaderIdx
	args := shardrpc.DeleteShardArgs{Shard: s, Num: num}
	ck.reqId++

	for {
		reply := shardrpc.DeleteShardReply{}
		ok := ck.clnt.Call(ck.servers[idx], "KVServer.Delete", &args, &reply)
		if ok {
			if reply.Err == rpc.OK {
				ck.leaderIdx = idx
				return rpc.OK
			}
			if reply.Err == rpc.ErrVersion {
				ck.leaderIdx = idx
				return rpc.ErrVersion
			}
			// Handle ErrWrongLeader by trying next server
			if reply.Err == rpc.ErrWrongLeader {
				idx = (idx + 1) % len(ck.servers)
				continue
			}
		}
		// RPC call failed, add delay before retrying
		if retryCount >= maxRetries {
			return rpc.ErrWrongGroup
		}
		retryCount++
		time.Sleep(10 * time.Millisecond)
		idx = (idx + 1) % len(ck.servers)
	}
}
