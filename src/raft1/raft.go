package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	"6.5840/tester1"
)

type term_t uint32
type log_index_t uint64

type logEntry struct {
	Term term_t
}

// Server's state type
type StateType int

const (
	Follower StateType = iota
	Candidate
	Leader
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	state       StateType
	currentTerm term_t
	log         []logEntry
	commitIndex int
	lastApplied int

	votedFor int  // candidateId that received vote in current term
	received bool // If received append in this cycle.

	// For a leader
	nextIndex  []int // for each server, index of the next log entry to send to that server
	matchIndex []int // For each server, index of the highest log entry known to be replicated on server

	applyChan chan raftapi.ApplyMsg
}

type AppendEntriesArgs struct {
	Term         term_t
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  term_t
	Entries      []logEntry // log entries to store(empty for heartbeat)
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    term_t
	Success bool
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
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
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
	// Your code here (3D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	CandidateID  int
	CurrentTerm  term_t
	LastLogIndex int
	LastLogTerm  term_t
}

// RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	Term        term_t
	VoteGranted bool // Vote for this candidate or not.
}

// Get the last log index from raft logs.
func (rf *Raft) getLastLogIndex() int {
	return len(rf.log) - 1
}

// Get the term of last log.
func (rf *Raft) getLastLogTerm() term_t {
	if len(rf.log) == 0 {
		return 0
	}
	return term_t(rf.log[len(rf.log)-1].Term)
}

func (rf *Raft) becomeCandidate() {
	rf.state = Candidate
	rf.currentTerm += 1
	rf.votedFor = rf.me
	fmt.Printf("[Node %v] Becomes candidate at term %v\n", rf.me, rf.currentTerm)
	rf.persist()
}

func (rf *Raft) becomeFollower(term term_t) {
	rf.state = Follower
	if term > rf.currentTerm {
		rf.currentTerm = term
		rf.votedFor = -1
	}
	rf.persist()
	rf.received = true

	fmt.Printf("[Node %v] Becomes follower at term %v\n", rf.me, rf.currentTerm)
}

func (rf *Raft) becomeLeader() {
	rf.state = Leader
	rf.votedFor = rf.me

	for i := range rf.peers {
		rf.nextIndex[i] = rf.getLastLogIndex() + 1
		rf.matchIndex[i] = 0
	}
	fmt.Printf("[Node %v] Becomes Leader at term %v\n", rf.me, rf.currentTerm)
	go rf.broadcastHeartbeats()
}

// Broadcast heartbeats to all peers.
func (rf *Raft) broadcastHeartbeats() {
	fmt.Printf("[Node %v] broadcase heartbeats, in term %v.\n", rf.me, rf.currentTerm)

	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderID:     rf.me,
		PrevLogIndex: rf.getLastLogIndex(),
		PrevLogTerm:  rf.getLastLogTerm(),
		Entries:      []logEntry{},
		LeaderCommit: rf.commitIndex,
	}

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(i int, args AppendEntriesArgs) {
			var reply AppendEntriesReply
			rf.sendAppendEntries(i, &args, &reply)
		}(i, args)
	}
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

	// Become a follower.
	if args.CurrentTerm > rf.currentTerm {
		rf.becomeFollower(args.CurrentTerm)
	}

	// If log is up-to-dated.
	upToDate := (args.LastLogTerm > rf.getLastLogTerm()) ||
		(args.LastLogTerm == rf.getLastLogTerm() && args.LastLogIndex >= rf.getLastLogIndex())

	fmt.Printf("[Node %v] received vote req from %v at term %v ", rf.me, args.CandidateID, args.CurrentTerm)
	fmt.Printf("rf.votedFor = %v, upToDate = %v\n", rf.votedFor, upToDate)
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateID) && upToDate {
		// Vote for this candidate.
		rf.votedFor = args.CandidateID
		rf.received = true
		rf.persist()
		reply.VoteGranted = true
		fmt.Printf("[Node %v] Votes for %v at term %v\n", rf.me, args.CandidateID, args.CurrentTerm)
	} else {
		// Refuse to vote.
		reply.VoteGranted = false
		fmt.Printf("[Node %v] refuse to vote for %v at term %v\n", rf.me, args.CandidateID, args.CurrentTerm)
	}

	reply.Term = rf.currentTerm
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply, count *int) {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		if reply.VoteGranted {
			*count++
			fmt.Printf("[Node %v] received vote from %v at term %v, count = %v\n", rf.me, server, args.CurrentTerm, *count)
			if *count > len(rf.peers)/2 && rf.state == Candidate {
				rf.becomeLeader()
			}
		} else if reply.Term > rf.currentTerm {
			rf.becomeFollower(reply.Term)
		}
	} else {
		fmt.Printf("[Node %v] failed to send request vote to %v\n", rf.me, server)
	}
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	arg := RequestVoteArgs{
		CandidateID:  rf.me,
		CurrentTerm:  rf.currentTerm,
		LastLogIndex: rf.getLastLogIndex(),
		LastLogTerm:  term_t(rf.getLastLogTerm()),
	}
	rf.mu.Unlock()

	var count int = 1

	for i := 0; i < len(rf.peers); i++ {
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).

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

func (rf *Raft) ticker() {
	for !rf.killed() {
		rf.mu.Lock()
		timeout := !rf.received

		if rf.state == Leader {
			rf.broadcastHeartbeats()
		} else if timeout {
			rf.becomeCandidate()
			rf.mu.Unlock()
			rf.startElection()
			rf.mu.Lock()
		}

		rf.received = false
		rf.mu.Unlock()

		if rf.state == Leader {
			time.Sleep(time.Duration(50) * time.Millisecond)
		} else {
			ms := 150 + rand.Intn(151)
			time.Sleep(time.Duration(ms) * time.Millisecond)
		}
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
	rf.log = []logEntry{{Term: 0}}
	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.votedFor = -1
	rf.received = false

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	rf.applyChan = applyCh

	// Your initialization code here (3A, 3B, 3C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

// Append new entry to raft.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	fmt.Printf("[Node %v] received append entries from %v at term %v\n", rf.me, args.LeaderID, args.Term)

	// Old term.
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

	// todo: change "len(rf.log)"
	// Check if PrevLogIndex exists.
	if args.PrevLogIndex >= len(rf.log) {
		reply.Success = false
		return
	}

	rf.log = rf.log[:args.PrevLogIndex+1]
	rf.log = append(rf.log, args.Entries...)

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.getLastLogIndex())
	}

	rf.persist()
	reply.Success = true
}

// Send AppendEntries RPC to a server.
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
