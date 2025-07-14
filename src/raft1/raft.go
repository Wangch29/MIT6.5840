package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"
	//"log"
	"bytes"
	"log"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	"6.5840/tester1"
)

type logEntry struct {
	Term    int         // The term when this entry was received by the leader.
	Command interface{} // The command to be applied to the state machine.
}

// Server's state type
type StateType int

const (
	Follower StateType = iota
	Candidate
	Leader
)

// States need to be persistent.
type PersistentState struct {
	CurrentTerm       int
	VotedFor          int
	Log               []logEntry
	LastIncludedIndex int
	LastIncludedTerm  int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	state       StateType
	currentTerm int
	log         []logEntry
	commitIndex int
	lastApplied int

	votedFor int  // candidateId that received vote in current term
	received bool // If received append in this cycle.

	// For a leader
	nextIndex  []int // for each server, index of the next log entry to send to that server
	matchIndex []int // For each server, index of the highest log entry known to be replicated on server

	applyChan chan raftapi.ApplyMsg

	// For log compaction
	lastIncludedIndex int
	lastIncludedTerm  int
}

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []logEntry // log entries to store(empty for heartbeat)
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	// Optional for log backtracking:
	ConflictIndex int
	ConflictTerm  int
	ConflictLen   int
}

// InstallSnapshot RPC args.
type InstallSnapshotArgs struct {
	Term              int
	LeaderID          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

// InstallSnapshot RPC reply.
type InstallSnapshotReply struct {
	Term int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool

	rf.mu.Lock()
	term = int(rf.currentTerm)
	isleader = (rf.state == Leader)
	rf.mu.Unlock()

	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
// Call it with holding mutex.
func (rf *Raft) persist() {
	rf.persistSnapshot(rf.persister.ReadSnapshot())
}

func (rf *Raft) persistSnapshot(snapshot []byte) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(PersistentState{
		CurrentTerm:       rf.currentTerm,
		VotedFor:          rf.votedFor,
		Log:               rf.log,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
	})
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, snapshot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if len(data) == 0 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var ps PersistentState
	if d.Decode(&ps) != nil {
		log.Printf("Fail to decode.\n")
		return
	} else {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		rf.currentTerm = ps.CurrentTerm
		rf.votedFor = ps.VotedFor
		rf.log = ps.Log
		rf.lastIncludedIndex = ps.LastIncludedIndex
		rf.lastIncludedTerm = ps.LastIncludedTerm
	}
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if index <= rf.lastIncludedIndex || index > rf.commitIndex {
		return
	}

	rf.lastIncludedTerm = rf.getLogTerm(index)
	// rf.log[index-rf.lastIncludedIndex] itself act as the dummy node.
	rf.log = append([]logEntry{}, rf.log[index-rf.lastIncludedIndex:]...)
	rf.lastIncludedIndex = index

	if index > rf.commitIndex {
		rf.commitIndex = index
	}
	if index > rf.lastApplied {
		rf.lastApplied = index
	}

	rf.persistSnapshot(snapshot)
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	CandidateID  int
	CurrentTerm  int
	LastLogIndex int
	LastLogTerm  int
}

// RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	Term        int
	VoteGranted bool // Vote for this candidate or not.
}

// Get the last log index from raft logs.
func (rf *Raft) getLastLogIndex() int {
	return rf.lastIncludedIndex + len(rf.log) - 1
}

// Get the term of last log.
func (rf *Raft) getLastLogTerm() int {
	if len(rf.log) == 0 {
		return 0
	}
	return rf.log[len(rf.log)-1].Term
}

// Get the term of log at given index.
func (rf *Raft) getLogTerm(index int) int {
	if index == rf.lastIncludedIndex {
		return rf.lastIncludedTerm
	}
	idx := index - rf.lastIncludedIndex
	if idx < 0 || idx >= len(rf.log) {
		return -1
	}
	return rf.log[idx].Term
}

func (rf *Raft) becomeCandidate() {
	rf.state = Candidate
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.persist()
}

func (rf *Raft) becomeFollower(term int) {
	rf.state = Follower
	if term > rf.currentTerm {
		rf.currentTerm = term
		rf.votedFor = -1
	}
	rf.persist()
	rf.received = true
}

func (rf *Raft) becomeLeader() {
	rf.mu.Lock()
	rf.state = Leader
	rf.votedFor = rf.me

	for i := range rf.peers {
		rf.nextIndex[i] = rf.getLastLogIndex() + 1
		rf.matchIndex[i] = 0
	}
	rf.matchIndex[rf.me] = rf.getLastLogIndex()
	// log.Printf("[Node %v] Becomes Leader at term %v\n", rf.me, rf.currentTerm)
	rf.mu.Unlock()
}

// Update commit index based on matchIndex.
// It should be called with holding the lock.
func (rf *Raft) updateCommitIndex() {
	if rf.state != Leader {
		return
	}

	// Find the highest matchIndex among all peers.
	rf.matchIndex[rf.me] = rf.getLastLogIndex()
	matchIndexCopy := make([]int, len(rf.matchIndex))
	copy(matchIndexCopy, rf.matchIndex)
	sort.Ints(matchIndexCopy) // todo: improve it.

	index := matchIndexCopy[len(matchIndexCopy)/2]

	// Check: Only
	if index > rf.commitIndex && rf.getLogTerm(index) == rf.currentTerm {
		rf.commitIndex = index
		// log.Printf("[Node %v] Updated commit index to %v at term %v\n", rf.me, rf.commitIndex, rf.currentTerm)
	}
}

// Broadcast heartbeats to all peers.
// Enter this function without holding the lock.
func (rf *Raft) broadcastHeartbeats() {
	for i := range rf.peers {
		if rf.state != Leader {
			// "If a leader or candidate discovers a server with a higher term, it immediately reverts to follower state."
			return
		}
		if i == rf.me {
			continue
		}

		go func(i int) {
			for {
				rf.mu.Lock()
				if rf.state != Leader {
					rf.mu.Unlock()
					return
				}

				nextIndex := rf.nextIndex[i]

				// If nextIndex falls behind rf.lastIncludedIndex, send snapshot.
				if nextIndex <= rf.lastIncludedIndex {
					args := InstallSnapshotArgs{
						Term:              rf.currentTerm,
						LeaderID:          rf.me,
						LastIncludedIndex: rf.lastIncludedIndex,
						LastIncludedTerm:  rf.lastIncludedTerm,
						Data:              rf.persister.ReadSnapshot(),
					}

					rf.mu.Unlock()
					var reply InstallSnapshotReply
					ok := rf.sendInstallSnapshot(i, &args, &reply)
					for !ok && rf.state == Leader {
						if rf.killed() {
							return
						}
						ok = rf.sendInstallSnapshot(i, &args, &reply) // Retry until success
					}
					rf.mu.Lock()
					if rf.state != Leader || rf.killed() {
						rf.mu.Unlock()
						return
					}

					if reply.Term > rf.currentTerm {
						rf.becomeFollower(reply.Term)
					} else {
						rf.matchIndex[i] = rf.lastIncludedIndex
						rf.nextIndex[i] = rf.lastIncludedIndex + 1
					}
					rf.mu.Unlock()
					return
				}

				var logEntries []logEntry
				idx := nextIndex - rf.lastIncludedIndex
				if idx < len(rf.log) {
					logEntries = make([]logEntry, len(rf.log[idx:]))
					copy(logEntries, rf.log[idx:])
				}

				args := AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderID:     rf.me,
					PrevLogIndex: max(rf.nextIndex[i]-1, 0),
					PrevLogTerm:  rf.getLogTerm(rf.nextIndex[i] - 1),
					Entries:      logEntries,
					LeaderCommit: rf.commitIndex,
				}

				rf.mu.Unlock()
				var reply AppendEntriesReply
				ok := rf.sendAppendEntries(i, &args, &reply)

				for !ok && rf.state == Leader {
					if rf.killed() {
						return
					}
					ok = rf.sendAppendEntries(i, &args, &reply) // Retry sending.
				}
				rf.mu.Lock()
				if rf.state != Leader || rf.killed() {
					rf.mu.Unlock()
					return
				}

				// Check if follower has higher term.
				if reply.Term > rf.currentTerm {
					rf.becomeFollower(reply.Term)
					rf.mu.Unlock()
					return
				}
				// If follower's log is up-to-date, update nextIndex and matchIndex.
				if reply.Success {
					// log.Printf("[Node %v] Append entries to Node [%v] at term %v\n", rf.me, i, rf.currentTerm)
					rf.matchIndex[i] = args.PrevLogIndex + len(args.Entries)
					rf.nextIndex[i] = rf.matchIndex[i] + 1
					rf.updateCommitIndex()
					rf.mu.Unlock()
					return
				} else if rf.state != Leader {
					// If this node is not leader anymore, return.
					rf.mu.Unlock()
					return
				} else {
					// Unsuccessful, decrease nextIndex and retry.
					if reply.ConflictTerm != -1 {
						has, idx := rf.findTerm(reply.ConflictTerm)
						if has {
							rf.nextIndex[i] = idx + 1
						} else {
							rf.nextIndex[i] = reply.ConflictIndex
						}
					} else {
						rf.nextIndex[i] = reply.ConflictLen
					}
					rf.mu.Unlock()
					// keep looping until success.
				}
			}
		}(i)
	}
}

// Find the last index with the term.
func (rf *Raft) findTerm(term int) (bool, int) {
	for i := len(rf.log) - 1; i >= 0; i-- {
		t := rf.log[i].Term
		if t == term {
			return true, i + rf.lastIncludedIndex
		}
		if t < term {
			return false, -1
		}
	}
	return false, -1
}

// RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Term is lower, refuse to vote.
	if args.CurrentTerm < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	reply.Term = rf.currentTerm

	// Become a follower.
	if args.CurrentTerm > rf.currentTerm {
		rf.becomeFollower(args.CurrentTerm)
	} else if args.CurrentTerm == rf.currentTerm && rf.state == Candidate {
		// To avoid election deadlock, a candidate may voluntarily step down to follower
		// when receiving a RequestVote RPC with the same term. This is not required by
		// the Raft spec, but helps convergence in practice.
		rf.becomeFollower(args.CurrentTerm)
	}

	// If log is up-to-dated.
	upToDate := (args.LastLogTerm > rf.getLastLogTerm()) ||
		(args.LastLogTerm == rf.getLastLogTerm() && args.LastLogIndex >= rf.getLastLogIndex())

	// log.Printf("[Node %v] received vote req from %v at term %v ", rf.me, args.CandidateID, args.CurrentTerm)
	// log.Printf("rf.votedFor = %v, upToDate = %v\n", rf.votedFor, upToDate)

	if upToDate {
		rf.received = true
		if rf.votedFor == -1 || rf.votedFor == args.CandidateID {
			// Vote for this candidate, and reset rf.received.
			rf.votedFor = args.CandidateID
			reply.VoteGranted = true
			rf.persist()
			// log.Printf("[Node %v] Votes for %v at term %v\n", rf.me, args.CandidateID, args.CurrentTerm)
			return
		}
	}
	// Refuse to vote.
	reply.VoteGranted = false
	// log.Printf("[Node %v] refuse to vote for %v at term %v\n", rf.me, args.CandidateID, args.CurrentTerm)
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply, count *int32) {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if ok {
		rf.mu.Lock()

		if reply.VoteGranted {
			atomic.AddInt32(count, 1)
			// log.Printf("[Node %v] received vote from %v at term %v, count = %v\n", rf.me, server, args.CurrentTerm, atomic.LoadInt32(count))
			if rf.state == Candidate && atomic.LoadInt32(count) > int32(len(rf.peers)/2) {
				rf.mu.Unlock()
				rf.becomeLeader()
				rf.mu.Lock()
			}
		} else if reply.Term > rf.currentTerm {
			rf.becomeFollower(reply.Term)
		}

		rf.mu.Unlock()
	} else {
		// log.Printf("[Node %v] failed to send request vote to %v\n", rf.me, server)
	}
}

func (rf *Raft) startElection() {
	arg := RequestVoteArgs{
		CandidateID:  rf.me,
		CurrentTerm:  rf.currentTerm,
		LastLogIndex: rf.getLastLogIndex(),
		LastLogTerm:  rf.getLastLogTerm(),
	}

	var count int32 = 1

	for i := 0; i < len(rf.peers); i++ {
		if rf.state != Candidate {
			break
		}
		if i == rf.me {
			continue
		}
		var reply RequestVoteReply
		go rf.sendRequestVote(i, &arg, &reply, &count)
	}
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := -1
	term := -1
	isLeader := true

	// Check if this server is the leader.
	if rf.state != Leader {
		// log.Printf("[Node %v] is not leader, cannot start command.\n", rf.me)
		isLeader = false
		return index, term, isLeader
	}
	// log.Printf("[Node %v] start command %v successfully.\n", rf.me, command)

	entry := logEntry{
		Term:    rf.currentTerm,
		Command: command,
	}

	rf.log = append(rf.log, entry)
	index = rf.getLastLogIndex()
	term = rf.getLastLogTerm()

	rf.persist()

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ElectionTicker() {
	for !rf.killed() {
		rf.mu.Lock()

		// log.Printf("[Node %v] ticker started, state: %v, received:%v\n", rf.me, rf.state, rf.received)

		if rf.state != Leader && !rf.received {
			rf.becomeCandidate()
			rf.startElection()
		}
		rf.received = false
		rf.mu.Unlock()

		// waits for a random time between 150ms and 300ms.
		ms := 150 + rand.Intn(151)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// This function is used to broadcast heartbeats periodically.
func (rf *Raft) broadcastTicker() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.state == Leader {
			rf.broadcastHeartbeats()
		}
		rf.mu.Unlock()
		time.Sleep(50 * time.Millisecond)
	}
}

func (rf *Raft) applyTicker() {
	for !rf.killed() {
		msgs := make([]raftapi.ApplyMsg, 0)

		rf.mu.Lock()
		for rf.lastApplied < rf.commitIndex && (rf.lastApplied-rf.lastIncludedIndex+1) < len(rf.log) &&
			(rf.lastApplied-rf.lastIncludedIndex) >= 0 {
			rf.lastApplied += 1
			msgs = append(msgs, raftapi.ApplyMsg{
				CommandValid: true,
				Command:      rf.log[rf.lastApplied-rf.lastIncludedIndex].Command,
				CommandIndex: rf.lastApplied,
			})
		}
		rf.mu.Unlock()

		for _, msg := range msgs {
			rf.applyChan <- msg
		}

		time.Sleep(25 * time.Millisecond)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.state = Follower
	rf.currentTerm = 0
	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.votedFor = -1
	rf.received = false

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	rf.applyChan = applyCh

	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.commitIndex = rf.lastIncludedIndex
	rf.lastApplied = rf.lastIncludedIndex

	if len(rf.log) == 0 {
		rf.log = []logEntry{{Term: rf.lastIncludedTerm, Command: nil}}
	}

	// start a ticker goroutine to start elections
	go rf.ElectionTicker()
	// start a broadcast goroutine to broadcast.
	go rf.broadcastTicker()
	// start a goroutine to apply committed log entries to the state machine
	go rf.applyTicker()

	return rf
}

// Append new entry to raft.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// log.Printf("[Node %v] received append entries from %v at term %v\n", rf.me, args.LeaderID, args.Term)

	// leader's term is too old.
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	rf.received = true

	if args.Term >= rf.currentTerm && rf.state != Follower {
		rf.becomeFollower(args.Term)
	}

	reply.Term = rf.currentTerm

	// Check if PrevLogIndex exists.
	if args.PrevLogIndex > rf.getLastLogIndex() {
		reply.Success = false
		reply.ConflictTerm = -1
		reply.ConflictLen = len(rf.log)
		return
	}
	// Check logEntry's term.
	if rf.getLogTerm(args.PrevLogIndex) != args.PrevLogTerm {
		reply.Success = false
		reply.ConflictTerm = rf.getLogTerm(args.PrevLogIndex)
		reply.ConflictIndex = rf.findConflictIndex(args.PrevLogIndex, reply.ConflictTerm)
		return
	}

	rf.log = append(rf.log[:args.PrevLogIndex+1-rf.lastIncludedIndex], args.Entries...)
	rf.persist()

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.getLastLogIndex())
	}

	reply.Success = true
}

// find the first entry with conflictTerm.
func (rf *Raft) findConflictIndex(conflictIndex int, conflictTerm int) int {
	for i := conflictIndex - 1; i >= rf.lastIncludedIndex; i-- {
		if rf.log[i-rf.lastIncludedIndex].Term != conflictTerm {
			break
		}
		conflictIndex = i
	}
	return conflictIndex
}

// Send AppendEntries RPC to a server.
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		return
	}

	rf.received = true

	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}

	reply.Term = rf.currentTerm

	// Snapshot is stale or redundant.
	if args.LastIncludedIndex <= rf.lastIncludedIndex {
		rf.mu.Unlock()
		return
	}

	// Reset and apply snapshot.
	idx := args.LastIncludedIndex - rf.lastIncludedIndex
	if idx < len(rf.log) {
		rf.log = append([]logEntry{}, rf.log[idx:]...)
	} else {
		rf.log = make([]logEntry, 1) // dummy node.
	}

	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	rf.lastApplied = max(args.LastIncludedIndex, rf.lastApplied)
	rf.commitIndex = max(args.LastIncludedIndex, rf.commitIndex)

	rf.persistSnapshot(args.Data)

	rf.mu.Unlock()

	go func(snapshot []byte, index, term int) {
		rf.applyChan <- raftapi.ApplyMsg{
			SnapshotValid: true,
			Snapshot:      snapshot,
			SnapshotTerm:  term,
			SnapshotIndex: index,
		}
	}(args.Data, args.LastIncludedIndex, args.LastIncludedTerm)
}
