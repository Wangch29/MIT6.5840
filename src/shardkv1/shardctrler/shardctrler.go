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
const next_config_key_name string = "next_shard_config"

// ShardCtrler for the controller and kv clerk.
type ShardCtrler struct {
	clnt *tester.Clnt
	kvtest.IKVClerk

	killed int32 // set by Kill()
	leases bool

	grpsClerks map[tester.Tgid]*shardgrp.Clerk // one clerk for each shard group
}

// ensureGrpClerksFor makes sure sck.grpsClerks has a clerk for every group in cfg.
func (sck *ShardCtrler) ensureGrpClerksFor(cfg *shardcfg.ShardConfig) {
	for gid, servers := range cfg.Groups {
		if _, ok := sck.grpsClerks[gid]; ok {
			continue
		}
		if len(servers) == 0 {
			sck.grpsClerks[gid] = nil
		} else {
			sck.grpsClerks[gid] = shardgrp.MakeClerk(sck.clnt, servers)
		}
	}
}

// finishReconfig migrates shards from old -> new (idempotent via Num) and then
// sets the controller's current config to new by CAS on config_key_name using curVersion.
func (sck *ShardCtrler) finishReconfig(old *shardcfg.ShardConfig, new *shardcfg.ShardConfig, curVersion rpc.Tversion) rpc.Err {
	// Create clerks for all groups mentioned in either config (needed after a crash).
	sck.ensureGrpClerksFor(old)
	sck.ensureGrpClerksFor(new)

	// Migrate shards that change ownership.
	var wg sync.WaitGroup
	for i := 0; i < shardcfg.NShards; i++ {
		shid := shardcfg.Tshid(i)
		oldGrpId := old.Shards[shid]
		newGrpId := new.Shards[shid]
		if oldGrpId == newGrpId {
			continue
		}
		wg.Add(1)
		go func(shid shardcfg.Tshid, oldGrpId, newGrpId tester.Tgid) {
			defer wg.Done()
			// 1) Freeze on old owner using old.Num. Retry until OK, or accept ErrVersion as "already advanced".
			var shardBytes []byte
			skipInstall := false
			for {
				b, err := sck.grpsClerks[oldGrpId].Freeze(shid, old.Num)
				if err == rpc.OK {
					shardBytes = b
					break
				}
				if err == rpc.ErrVersion {
					// Old group reports a newer state; treat as already frozen/moved.
					skipInstall = true
					break
				}
				time.Sleep(100 * time.Millisecond)
			}

			// 2) Install on new owner with new.Num. Only if we have bytes to install.
			if !skipInstall {
				for {
					err := sck.grpsClerks[newGrpId].InstallShard(shid, shardBytes, new.Num)
					if err == rpc.OK || err == rpc.ErrVersion {
						break
					}
					time.Sleep(100 * time.Millisecond)
				}
			}

			// 3) Delete on old owner with new.Num (idempotent). Accept OK or ErrVersion as success.
			for {
				err := sck.grpsClerks[oldGrpId].Delete(shid, new.Num)
				if err == rpc.OK || err == rpc.ErrVersion {
					break
				}
				time.Sleep(100 * time.Millisecond)
			}
		}(shid, oldGrpId, newGrpId)
	}
	wg.Wait()

	// Drop clerks for groups that no longer exist in the new config.
	for gid := range old.Groups {
		if _, ok := new.Groups[gid]; !ok {
			delete(sck.grpsClerks, gid)
		}
	}

	// Atomically advance current config from old -> new using CAS with curVersion.
	for {
		if err := sck.IKVClerk.Put(config_key_name, new.String(), curVersion); err == rpc.OK {
			break
		}
		if err := func() rpc.Err {
			_, v, e := sck.IKVClerk.Get(config_key_name)
			if e == rpc.OK && v == curVersion+1 {
				return rpc.OK
			}
			return rpc.ErrMaybe
		}(); err == rpc.OK {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	return rpc.OK
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
	curVal, curVer, _ := sck.IKVClerk.Get(config_key_name)
	nextVal, _, _ := sck.IKVClerk.Get(next_config_key_name)
	curCfg := shardcfg.FromString(curVal)
	nextCfg := shardcfg.FromString(nextVal)

	if nextCfg.Num > curCfg.Num {
		// A previous controller started a reconfig but didn't finish. Complete it now.
		return sck.finishReconfig(curCfg, nextCfg, curVer)
	}
	// Nothing to recover; either equal or next is stale.
	return rpc.OK
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
	sck.IKVClerk.Put(config_key_name, cfg.String(), 0)
	sck.IKVClerk.Put(next_config_key_name, cfg.String(), 0)
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

	// Write new config.
	for {
		_, ver, _ := sck.IKVClerk.Get(next_config_key_name)
		err := sck.IKVClerk.Put(next_config_key_name, new.String(), ver)
		if err == rpc.OK {
			break
		}
		if err == rpc.ErrMaybe {
			_, v, err := sck.IKVClerk.Get(next_config_key_name)
			if err == rpc.OK && v == ver+1 {
				break
			}
		}
		time.Sleep(100 * time.Millisecond)
	}

	// Ensure we have clerks for any new groups before migration (finishReconfig also ensures).
	sck.ensureGrpClerksFor(new)

	// Perform the migration and advance current config.
	_ = sck.finishReconfig(old, new, version)

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
		val, version, err := sck.IKVClerk.Get(config_key_name)
		if err == rpc.OK {
			return shardcfg.FromString(val), version
		}
		// Add delay to avoid overwhelming the server
		time.Sleep(100 * time.Millisecond)
	}
}
