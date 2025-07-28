package kvraft

import (
	"math/rand"

	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"6.5840/tester1"
)

type Clerk struct {
	clnt      *tester.Clnt
	servers   []string
	leaderIdx int
	clerkId   int64
	reqId     int64
}

func MakeClerk(clnt *tester.Clnt, servers []string) kvtest.IKVClerk {
	ck := &Clerk{clnt: clnt, servers: servers}
	ck.servers = append([]string{}, servers...)
	ck.leaderIdx = 0
	ck.clerkId = int64(rand.Int63()) // Randomly generated client ID
	ck.reqId = 0
	return ck
}

// Get fetches the current value and version for a key.  It returns
// ErrNoKey if the key does not exist. It keeps trying forever in the
// face of all other errors.
//
// You can send an RPC to server i with code like this:
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Get", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	idx := ck.leaderIdx
	args := rpc.GetArgs{Key: key}

	for {
		reply := rpc.GetReply{}

		ok := ck.clnt.Call(ck.servers[idx], "KVServer.Get", &args, &reply)

		if ok {
			if reply.Err == rpc.OK {
				ck.leaderIdx = idx
				return reply.Value, reply.Version, reply.Err
			}
			if reply.Err == rpc.ErrNoKey {
				ck.leaderIdx = idx
				return "", 0, reply.Err
			}
		}
		idx = (idx + 1) % len(ck.servers)
	}
}

// Put updates key with value only if the version in the
// request matches the version of the key at the server.  If the
// versions numbers don't match, the server should return
// ErrVersion.  If Put receives an ErrVersion on its first RPC, Put
// should return ErrVersion, since the Put was definitely not
// performed at the server. If the server returns ErrVersion on a
// resend RPC, then Put must return ErrMaybe to the application, since
// its earlier RPC might have been processed by the server successfully
// but the response was lost, and the the Clerk doesn't know if
// the Put was performed or not.
//
// You can send an RPC to server i with code like this:
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Put", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	idx := ck.leaderIdx
	args := rpc.PutArgs{Key: key, Value: value, Version: version, ClientId: ck.clerkId, ReqId: ck.reqId}
	ck.reqId++
	first := true

	for {
		reply := rpc.PutReply{}

		ok := ck.clnt.Call(ck.servers[idx], "KVServer.Put", &args, &reply)

		if ok {
			if reply.Err == rpc.OK {
				ck.leaderIdx = idx
				return reply.Err
			}
			if reply.Err == rpc.ErrNoKey {
				ck.leaderIdx = idx
				return rpc.ErrNoKey
			}
			if reply.Err == rpc.ErrVersion {
				if first {
					ck.leaderIdx = idx
					return rpc.ErrVersion
				} else {
					return rpc.ErrMaybe
				}
			}
		}
		first = false
		idx = (idx + 1) % len(ck.servers)
	}
}
