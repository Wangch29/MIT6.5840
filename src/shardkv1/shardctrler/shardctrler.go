package shardctrler

//
// Shardctrler with InitConfig, Query, and ChangeConfigTo methods
//

import (
	"sync"
	"sync/atomic"
	"time"

	kvsrv "6.5840/kvsrv1"
	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp"
	tester "6.5840/tester1"
)

// key name for shard configuration
const config_key_name string = "shard_config"

// ShardCtrler for the controller and kv clerk.
type ShardCtrler struct {
	clnt *tester.Clnt
	kvtest.IKVClerk

	killed int32 // set by Kill()
	leases bool

	grpsClerks map[tester.Tgid]*shardgrp.Clerk // one clerk for each shard group
}

// Make a ShardCltler, which stores its state in a kvsrv.
func MakeShardCtrler(clnt *tester.Clnt, leases bool) *ShardCtrler {
	sck := &ShardCtrler{clnt: clnt, leases: leases}
	srv := tester.ServerName(tester.GRP0, 0)
	sck.IKVClerk = kvsrv.MakeClerk(clnt, srv)
	sck.grpsClerks = make(map[tester.Tgid]*shardgrp.Clerk)
	return sck
}

// The tester calls InitController() before starting a new
// controller. In part A, this method doesn't need to do anything. In
// B and C, this method implements recovery (part B) and uses a lock
// to become leader (part C). InitController should return
// rpc.ErrVersion when another controller supersedes it (e.g., when
// this controller is partitioned during recovery); this happens only
// in Part C. Otherwise, it returns rpc.OK.
func (sck *ShardCtrler) InitController() rpc.Err {
	return rpc.ErrVersion
}

// The tester calls ExitController to exit a controller. In part B and
// C, release lock.
func (sck *ShardCtrler) ExitController() {
}

// Called once by the tester to supply the first configuration.  You
// can marshal ShardConfig into a string using shardcfg.String(), and
// then Put it in the kvsrv for the controller at version 0.  You can
// pick the key to name the configuration.
func (sck *ShardCtrler) InitConfig(cfg *shardcfg.ShardConfig) {
	err := sck.IKVClerk.Put("shard_config", cfg.String(), 0)
	for err != rpc.OK {
		err = sck.IKVClerk.Put("shard_config", cfg.String(), 0)
	}
	// Initialize the group clerks for each group
	for gid, servers := range cfg.Groups {
		if len(servers) == 0 {
			sck.grpsClerks[gid] = nil // no servers for this shard
		} else {
			sck.grpsClerks[gid] = shardgrp.MakeClerk(sck.clnt, servers)
		}
	}
}

// Called by the tester to ask the controller to change the
// configuration from the current one to new. It should return
// rpc.ErrVersion if this controller is superseded by another
// controller, as in part C.  In all other cases, it should return
// rpc.OK.
func (sck *ShardCtrler) ChangeConfigTo(new *shardcfg.ShardConfig) rpc.Err {
	old, version := sck.Query()

	if new.Num != old.Num+1 {
		return rpc.ErrVersion
	}

	// Try to add new the group clerks.
	for gid, servers := range new.Groups {
		oldServers, ok := old.Groups[gid]
		if !ok {
			// add a new group
			sck.grpsClerks[gid] = shardgrp.MakeClerk(sck.clnt, servers)
		} else {
			// old group, check if servers are changed.
			for idx, srv := range servers {
				if oldServers[idx] != srv {
					panic("ChangeConfigTo: do not support changing servers in a group")
				}
			}
		}
	}

	// Migrate shards.
	var wg sync.WaitGroup
	for i := 0; i < shardcfg.NShards; i++ {
		shid := shardcfg.Tshid(i)
		if new.Shards[shid] != old.Shards[shardcfg.Tshid(i)] {
			// Shard i is being moved to a different group.
			// We need to freeze the shard before installing the new configuration.
			oldGrpId := old.Shards[shid]
			newGrpId := new.Shards[shid]

			wg.Add(1)
			go func(shid shardcfg.Tshid, oldGrpId, newGrpId tester.Tgid) {
				defer wg.Done()
				// Freeze
				var shardBytes []byte
				var err rpc.Err
				for {
					shardBytes, err = sck.grpsClerks[oldGrpId].Freeze(shid, old.Num)
					if err == rpc.OK {
						break
					}
					time.Sleep(100 * time.Millisecond)
				}
				// Install
				for {
					err = sck.grpsClerks[newGrpId].InstallShard(shid, shardBytes, new.Num)
					if err == rpc.OK {
						break
					}
					time.Sleep(100 * time.Millisecond)
				}
				// Delete
				for {
					err = sck.grpsClerks[oldGrpId].Delete(shid, new.Num)
					if err == rpc.OK {
						break
					}
					time.Sleep(100 * time.Millisecond)
				}
			}(shid, oldGrpId, newGrpId)
		}
	}

	wg.Wait()

	// Try to delete old group clerks.
	for gid := range old.Groups {
		if _, ok := new.Groups[gid]; !ok {
			delete(sck.grpsClerks, gid)
		}
	}

	// Update the configuration in the kvsrv.
	for {
		err := sck.IKVClerk.Put(config_key_name, new.String(), version)
		if err == rpc.OK {
			break
		}
		if err == rpc.ErrMaybe {
			_, v, err := sck.IKVClerk.Get(config_key_name)
			if err == rpc.OK && v == version+1 {
				break
			}
		}
		time.Sleep(100 * time.Millisecond)
	}

	return rpc.OK
}

// Tester "kills" shardctrler by calling Kill().  For your
// convenience, we also supply isKilled() method to test killed in
// loops.
func (sck *ShardCtrler) Kill() {
	atomic.StoreInt32(&sck.killed, 1)
}

func (sck *ShardCtrler) isKilled() bool {
	z := atomic.LoadInt32(&sck.killed)
	return z == 1
}

// Return the current configuration and its version number
func (sck *ShardCtrler) Query() (*shardcfg.ShardConfig, rpc.Tversion) {
	for {
		val, version, err := sck.IKVClerk.Get("shard_config")
		if err == rpc.OK {
			return shardcfg.FromString(val), version
		}
		// Add delay to avoid overwhelming the server
		time.Sleep(100 * time.Millisecond)
	}
}
