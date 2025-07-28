package rsm

import (
	"sync"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	raft "6.5840/raft1"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

var useRaftStateMachine bool // to plug in another raft besided raft1

type Op struct {
	Id  int
	Me  int
	Req any
}

// A server (i.e., ../server.go) that wants to replicate itself calls
// MakeRSM and must implement the StateMachine interface.  This
// interface allows the rsm package to interact with the server for
// server-specific operations: the server must implement DoOp to
// execute an operation (e.g., a Get or Put request), and
// Snapshot/Restore to snapshot and restore the server's state.
type StateMachine interface {
	DoOp(any) any
	Snapshot() []byte
	Restore([]byte)
}

type RSM struct {
	mu           sync.Mutex
	me           int
	rf           raftapi.Raft
	applyCh      chan raftapi.ApplyMsg
	maxraftstate int // snapshot if log grows this big
	sm           StateMachine
	notifyCh     map[int]chan any
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// The RSM should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
//
// MakeRSM() must return quickly, so it should start goroutines for
// any long-running work.
func MakeRSM(servers []*labrpc.ClientEnd, me int, persister *tester.Persister, maxraftstate int, sm StateMachine) *RSM {
	rsm := &RSM{
		me:           me,
		maxraftstate: maxraftstate,
		applyCh:      make(chan raftapi.ApplyMsg),
		sm:           sm,
	}
	if !useRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
	}
	rsm.notifyCh = make(map[int]chan any)

	snapshot := persister.ReadSnapshot()
	if len(snapshot) > 0 {
		rsm.sm.Restore(snapshot)
	}

	go rsm.applier()
	return rsm
}

func (rsm *RSM) applier() {
	for msg := range rsm.applyCh {
		if msg.CommandValid {
			rsm.mu.Lock()
			op := msg.Command.(Op)
			ch, hasCh := rsm.notifyCh[msg.CommandIndex]
			me := rsm.me
			rsm.mu.Unlock()

			result := rsm.sm.DoOp(op.Req)
			if hasCh {
				if op.Me == me {
					ch <- result
				} else {
					ch <- nil // not my op, so return nil
				}
			}
		} else if msg.SnapshotValid {
			rsm.mu.Lock()
			rsm.sm.Restore(msg.Snapshot)
			rsm.mu.Unlock()
		}

		// Truncate the Raft log if it exceeds maxraftstate.
		if rsm.maxraftstate != -1 && rsm.rf.PersistBytes() > rsm.maxraftstate {
			snapshot := rsm.sm.Snapshot()
			rsm.rf.Snapshot(msg.CommandIndex, snapshot)
		}
	}

	for idx, ch := range rsm.notifyCh {
		// notify all waiting channels that the applyCh is closed
		// and they should return ErrWrongLeader
		ch <- nil
		delete(rsm.notifyCh, idx)
	}
}

func (rsm *RSM) Raft() raftapi.Raft {
	return rsm.rf
}

// Submit a command to Raft, and wait for it to be committed.  It
// should return ErrWrongLeader if client should find new leader and
// try again.
func (rsm *RSM) Submit(req any) (rpc.Err, any) {
	// Submit creates an Op structure to run a command through Raft;
	// for example: op := Op{Me: rsm.me, Id: id, Req: req}, where req
	// is the argument to Submit and id is a unique id for the op.
	op := Op{
		Me:  rsm.me,
		Req: req,
	}

	idx, _, isLeader := rsm.rf.Start(op)
	if !isLeader {
		return rpc.ErrWrongLeader, nil
	}

	rsm.mu.Lock()
	ch := make(chan any, 1)
	rsm.notifyCh[idx] = ch
	rsm.mu.Unlock()

	defer func() {
		rsm.mu.Lock()
		delete(rsm.notifyCh, idx)
		rsm.mu.Unlock()
	}()

	select {
	case result := <-ch:
		if result == nil {
			return rpc.ErrWrongLeader, nil
		}
		return rpc.OK, result
	case <-time.After(2000 * time.Millisecond):

		return rpc.ErrWrongLeader, nil
	}
}
