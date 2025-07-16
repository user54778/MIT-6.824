// Package Raft implements the Raft consensus algorithm for managing a replicated log.
package raft

import (
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

// Type Raft is a Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// NOTE: Persistent State: These fields must be stored to disk prior to responding to RPCs
	currentTerm int        // Latest term server has seen (a monotonic integer)
	votedFor    int        // The candidateID that received the vote in the currentTerm, or -1 if none.
	logEntries  []LogEntry // log[] The log entries to replicate (3B onward)

	// NOTE: Volatile State on all servers
	// TODO: 3B-3D
	commitIndex int
	lastApplied int
	// NOTE: Volatile state on leaders
	// TODO: 3B-3D
	nextIndex  []int
	matchIndex []int

	// Additional fields
	state     RaftState // State a given Raft replica is in (Follower, Candidate, or Leader)
	voteTotal int       // Total votes a replica has received

	hbChan chan bool
}

// Type raftstate represents one of three states at any given point
// a Raft replica may be in.
type RaftState int

const (
	Follower RaftState = iota
	Candidate
	Leader
)

// Type LogEntry represents a Raft Log. A Raft log stores a state machine command
// along with the term number when the entry was received by the Leader.
type LogEntry struct {
	Index   int // Position in the log
	Term    int // Term number entry was received by the leader
	Command any // State machine command
}

// GetState returns the currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == Leader
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
	// TODO: Your data here (3A, 3B).
	Term        int
	CandidateID int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// TODO: Your data here (3A).
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term     int
	LeaderID int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// RequestVote is an RPC handler called during the process of Leader Election.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// TODO: Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// You don't get MY vote: term < currentTerm
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// Whoever I am, I need to step down and become a follower since my term < the RPC term
	if rf.currentTerm < args.Term {
		rf.transitionFollower()
	}

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	// Otherwise, what will happen?
	// if votedFor is nil, or candidateID == votedFor, && log is at least as up to date as receiver's log
	// grant vote
	// for now, simply do first part as valid
	if rf.votedFor < 0 || rf.votedFor == args.CandidateID {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateID
		// NOTE: send indication of vote granted
	}
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// invalid state i can be in s.t. i shouldn't proceed?
	if rf.state != Candidate || args.Term != rf.currentTerm || reply.Term < rf.currentTerm {
		return
	}
	// go to follower?
	if reply.Term > rf.currentTerm {
		rf.transitionFollower()
		return
	}

	// if we recv VoteGranted?
	if reply.VoteGranted {
		rf.voteTotal++
		// send vote somehow
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// TODO: implement me!
	rf.hbChan <- true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	// TODO: implement me!
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
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
func (rf *Raft) Start(command any) (int, int, bool) {
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

func (rf *Raft) electionTimeout() int64 {
	// pause for a random amount of time between 50 and 350
	// milliseconds.
	// ms := 50 + (rand.Int63() % 300)
	// time.Sleep(time.Duration(ms) * time.Millisecond)
	return 50 + (rand.Int63() % 300)
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		// TODO: Your code here (3A)
		rf.mu.Lock()
		defer rf.mu.Unlock()
		Debug(logDebug, "S%d in rf.ticker()", rf.me)
		// time.Sleep(time.Duration(rf.getTimeout()) * time.Millisecond)
		switch rf.state {
		case Follower:
			select {
			case <-rf.hbChan:
				Debug(logDebug, "S%d received heartbeat", rf.me)
				// reset timeout
			default:
				time.Sleep(time.Duration(rf.electionTimeout()) * time.Millisecond)
				Debug(logInfo, "S%d timed out. Calling transitionCandidate()", rf.me)
				rf.transitionCandidate()
			}
		case Candidate:

		case Leader:
		}
	}
}

// transitionCandidate -> MUST be a Follower that has timed out, suspecting the Leader failed.
func (rf *Raft) transitionCandidate() {
	Debug(logDebug, "S%d in transitionCandidate", rf.me)
}

// transitionLeader -> MUST be a Candidate that receives values from quorum of nodes.
func (rf *Raft) transitionLeader() {}

// transitionFollower -> A new term was discovered; used by both Candidate and Leader
func (rf *Raft) transitionFollower() {}

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
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg,
) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	/*
		state RaftState // State a given Raft replica is in (Follower, Candidate, or Leader)

		// NOTE: Persistent State: These fields must be stored to disk prior to responding to RPCs
		currentTerm int        // Latest term server has seen (a monotonic integer)
		votedFor    int        // The candidateID that received the vote in the currentTerm, or -1 if none.
		logEntry    []LogEntry // log[] The log entries to replicate (3B onward)
	*/
	rf.state = Follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.voteTotal = 0
	rf.logEntries = []LogEntry{}

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.hbChan = make(chan bool)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	Debug(logDebug, "S%d Follower first initialized", rf.me)

	// start ticker goroutine to start elections
	go rf.ticker()

	// Hint 4?

	return rf
}
