package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client uses the shardctrler to query for the current
// configuration and find the assignment of shards (keys) to groups,
// and then talks to the group that holds the key's shard.
//

import (
	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardctrler"
	"6.5840/shardkv1/shardgrp"
	tester "6.5840/tester1"
)

type Clerk struct {
	clnt       *tester.Clnt
	sck        *shardctrler.ShardCtrler
	grpsClerks map[tester.Tgid]*shardgrp.Clerk // one clerk for each shard group
	Cfg        *shardcfg.ShardConfig           // shard group configuration for each shard
}

// The tester calls MakeClerk and passes in a shardctrler so that
// client can call it's Query method
func MakeClerk(clnt *tester.Clnt, sck *shardctrler.ShardCtrler) kvtest.IKVClerk {
	ck := &Clerk{
		clnt:       clnt,
		sck:        sck,
		grpsClerks: make(map[tester.Tgid]*shardgrp.Clerk),
	}

	cfg, _ := sck.Query()
	ck.Cfg = cfg

	for gid, servers := range cfg.Groups {
		if len(servers) == 0 {
			ck.grpsClerks[gid] = nil // no servers for this shard
		} else {
			ck.grpsClerks[gid] = shardgrp.MakeClerk(clnt, servers)
		}
	}

	return ck
}

// Get a key from a shardgrp.  You can use shardcfg.Key2Shard(key) to
// find the shard responsible for the key and ck.sck.Query() to read
// the current configuration and lookup the servers in the group
// responsible for key.  You can make a clerk for that group by
// calling shardgrp.MakeClerk(ck.clnt, servers).
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	for {
		// log.Printf("Clerk.Get: key=%s\n", key)

		cfg, _ := ck.sck.Query()
		if cfg.Num > ck.Cfg.Num {
			ck.updateConfig()
			continue
		}

		shId := shardcfg.Key2Shard(key)
		gid := ck.Cfg.Shards[shId]
		grpClerk := ck.grpsClerks[gid]
		n := ck.Cfg.Num

		if gid == 0 || len(cfg.Groups[gid]) == 0 {
			// All shards are assigned to invalid groups, return an error to allow graceful exit
			return "", 0, rpc.ErrNoKey
		}

		if grpClerk == nil {
			ck.updateConfig()
			continue
		}

		value, version, err := grpClerk.Get(key, n)

		if err == rpc.ErrWrongGroup {
			ck.updateConfig()
			continue
		}

		// Return the result, including ErrNoKey
		return value, version, err
	}
}

// Put a key to a shard group.
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	for {
		// log.Printf("Clerk.Put: key=%s, value=%s, version=%d\n", key, value, version)

		shId := shardcfg.Key2Shard(key)
		gid := ck.Cfg.Shards[shId]
		grpClerk := ck.grpsClerks[gid]

		if grpClerk == nil {
			ck.updateConfig()
			continue
		}

		if gid == 0 || len(ck.Cfg.Groups[gid]) == 0 {
			// All shards are assigned to invalid groups, return an error to allow graceful exit
			return rpc.ErrNoKey
		}

		_, err := grpClerk.Put(key, value, version, ck.Cfg.Num)

		if err == rpc.ErrWrongGroup {
			ck.updateConfig()
			continue
		}

		if err == rpc.ErrVersion {
			return rpc.ErrVersion
		}

		return err
	}
}

func (ck *Clerk) updateConfig() {
	cfg, _ := ck.sck.Query()
	ck.Cfg = cfg
	// Clear old grpsClerks mapping
	ck.grpsClerks = make(map[tester.Tgid]*shardgrp.Clerk)
	// cfg.Groups
	for gid := range cfg.Groups {
		servers := cfg.Groups[gid]
		if len(servers) == 0 {
			ck.grpsClerks[gid] = nil // no servers for this shard
		} else {
			ck.grpsClerks[gid] = shardgrp.MakeClerk(ck.clnt, servers)
		}
	}
}
