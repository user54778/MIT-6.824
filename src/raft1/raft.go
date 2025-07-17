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

	hbChan         chan bool
	sendVoteChan   chan bool
	winElectChan   chan bool
	transitionChan chan bool
}

// Type raftstate represents one of three states at any given point
// a Raft replica may be in.
type RaftState int

const (
	Follower RaftState = iota
	Candidate
	Leader
)

func (r RaftState) String() string {
	switch r {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "Unknown"
	}
}

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
	// must hold lock
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

// RequestVoteArgs represents the RequestVote RPC that is invoked
// by Candidate replicas during an election.
type RequestVoteArgs struct {
	Term        int // Candidate term
	CandidateID int // Candidate requesting vote
}

// RequestVoteReply represents the reply for a RequestVote RPC
// invoked during an election.
type RequestVoteReply struct {
	Term        int  // Current term
	VoteGranted bool // Flag for receiving vote
}

// AppendEntriesArgs is used by the Leader to replicate log entries,
// and send heartbeat messages to Follower replicas.
type AppendEntriesArgs struct {
	Term     int // Leader's term
	LeaderID int // To allow the follower to redirect clients
}

// AppendEntriesReply represents the reply for the AppendEntries RPC.
type AppendEntriesReply struct {
	Term    int  // Current term
	Success bool // Flag for matching entry w/ prevLogIndex and prevLogTerm (not yet impl'd)
}

// RequestVote is an RPC handler called during the process of Leader Election.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	Debug(logTrace, "S%d in RequestVote", rf.me)

	// Term in RPC is smaller than Candidate's. Reject.
	if args.Term < rf.currentTerm {
		Debug(logWarn, "S%d term %d greater than requesting term %d. Reject.", rf.me, rf.currentTerm, args.Term)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// Leader's term at least as large, step down to Follower state.
	if args.Term > rf.currentTerm {
		Debug(logTerm, "S%d term %d less than requesting term %d. Transitioning to follower...", rf.me, rf.currentTerm, args.Term)
		rf.transitionFollower(args.Term)
	}

	reply.Term = args.Term
	reply.VoteGranted = false

	Debug(logVote, "S%d setting reply as %v", rf.me, reply)

	// Should grant vote?
	if rf.votedFor < 0 || rf.votedFor == args.CandidateID {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateID
		Debug(logVote, "S%d granting vote to %d", rf.me, rf.votedFor)
		rf.sendValOnChannel(rf.sendVoteChan, true)
	}
	Debug(logDebug, "S%d sent on vote channel", rf.me)
}

// Send a NON-BLOCKING value on a channel. This is required since
// ticker() is not always ready.
func (rf *Raft) sendValOnChannel(ch chan bool, v bool) {
	select {
	case ch <- v:
		Debug(logInfo, "S%d sent val on channel", rf.me)
	default:
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
		Debug(logError, "RequestVote RPC failed")
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	Debug(logDebug, "S%d in sendRequestVote()", rf.me)

	// bad data
	if rf.state != Candidate || args.Term != rf.currentTerm || reply.Term < rf.currentTerm {
		Debug(logWarn, "S%d failure in sendRequestVote()", rf.me)
		return
	}

	// transition to follower
	if rf.currentTerm < reply.Term {
		Debug(logTerm, "S%d term < reply's term in sendRequestVote(), transitioning to Follower", rf.me)
		rf.transitionFollower(reply.Term)
		return
	}

	if reply.VoteGranted {
		rf.voteTotal++
		Debug(logVote, "S%d voteTotal: %d", rf.me, rf.voteTotal)
		// If we've received majority quorum, win election
		if rf.voteTotal == len(rf.peers)/2+1 {
			Debug(logVote, "S%d won election", rf.me)
			// rf.winElectChan <- true
			rf.sendValOnChannel(rf.winElectChan, true)
			// rf.transitionLeader()
		}
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	Debug(logLeader, "S%d term. Do something else here? Sending heartbeat...", rf.me)
	if rf.currentTerm > args.Term {
		Debug(logTerm, "S%d term > args term in AppendEntries(), transition to Follower", rf.me)
		reply.Term = rf.currentTerm
		return
	}

	Debug(logLeader, "S%d term. Do something else here? Sending heartbeat...", rf.me)
	// ???? NO!rf.currentTerm = reply.Term
	// rf.hbChan <- true
	rf.sendValOnChannel(rf.hbChan, true)
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if !ok {
		Debug(logError, "AppendEntries RPC failed")
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader {
		Debug(logWarn, "S%d in not leader", rf.me)
		return
	}
	if reply.Term < rf.currentTerm {
		Debug(logWarn, "S%d bad term %d %d", rf.me, reply.Term, rf.currentTerm)
		return
	}

	if rf.currentTerm < reply.Term {
		Debug(logLeader, "S%d should convert to follower", rf.me)
		rf.transitionFollower(reply.Term)
	}

	Debug(logDebug, "S%d OK in sendAppendEntries()", rf.me)
}

func (rf *Raft) broadcastRequestVote() {
	if rf.state != Candidate {
		Debug(logWarn, "S%d not a Candidate in broadcastRequestVote()", rf.me)
		return
	}

	args := RequestVoteArgs{
		Term:        rf.currentTerm,
		CandidateID: rf.me,
	}

	for s := range rf.peers {
		if s != rf.me {
			Debug(logTrace, "S%d sending vote to %d", rf.me, s)
			go rf.sendRequestVote(s, &args, &RequestVoteReply{})
		}
	}
}

// For now a simple method that broadcasts emtpy AppendEntries RPCs.
func (rf *Raft) sendHeartbeats() {
	// if term, ok := rf.GetState(); !ok {
	if rf.state != Leader {
		Debug(logWarn, "S%d called sendHeartbeats while in state:  %v", rf.me, rf.state)
		return
	}

	args := AppendEntriesArgs{}
	args.Term = rf.currentTerm
	args.LeaderID = rf.me
	for s := range rf.peers {
		if s != rf.me {
			// send heartbeat
			reply := AppendEntriesReply{}
			go rf.sendAppendEntries(s, &args, &reply)
		}
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

// Pause for a random amount of time between 50 and 350
// milliseconds.
func (rf *Raft) electionTimeout() int64 {
	return 150 + (rand.Int63() % 300)
}

// Compute a heartbeatTimeout << electionTimeout
func (rf *Raft) heartbeatTimeout() int64 {
	return max(rf.electionTimeout()/10, 50)
}

func (rf *Raft) ticker() {
	// NOTE: The ticker HAS to block for EVERYTHING, or else we will never actually
	// transition into any state and always timeout.
	for !rf.killed() {
		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()
		switch state {
		case Follower:
			select {
			case <-rf.hbChan:
				Debug(logDebug, "S%d follower received heartbeat", rf.me)
			case <-rf.sendVoteChan:
				Debug(logDebug, "S%d follower received vote chan", rf.me)
			case <-time.After(time.Duration(rf.electionTimeout()) * time.Millisecond):
				Debug(logInfo, "S%d timed out. Calling transitionCandidate()", rf.me)
				rf.transitionCandidate(Follower)
			}
		case Candidate:
			select {
			case <-rf.winElectChan:
				Debug(logDebug, "S%d candidate new Leader", rf.me)
				rf.transitionLeader()
			case <-rf.transitionChan:
				Debug(logDebug, "S%d candidate transition", rf.me)
			case <-time.After(time.Duration(rf.electionTimeout()) * time.Millisecond):
				Debug(logInfo, "S%d timed out. Calling transitionCandidate() in Candidate", rf.me)
				rf.transitionCandidate(Candidate)
			}
		case Leader:
			select {
			case <-rf.transitionChan:
				Debug(logDebug, "S%d leader transition", rf.me)
			case <-time.After(time.Duration(rf.heartbeatTimeout()) * time.Millisecond):
				Debug(logInfo, "S%d send heartbeat", rf.me)
				rf.mu.Lock()
				rf.sendHeartbeats()
				rf.mu.Unlock()
			}
		}
	}
}

// transitionFollower -> A new term was discovered; used by both Candidate and Leader
func (rf *Raft) transitionFollower(term int) {
	// must hold lock when call
	Debug(logDebug, "S%d transitionFollower. Current state: %s", rf.me, rf.state)
	state := rf.state

	rf.currentTerm = term
	rf.state = Follower
	rf.votedFor = -1

	// NOTE: this is required or we run into weird race condition
	if state != Follower {
		Debug(logError, "S%d should already be follower", rf.me)
		rf.sendValOnChannel(rf.transitionChan, true)
	}
}

// transitionCandidate -> MUST be a Follower that has timed out, suspecting the Leader failed.
func (rf *Raft) transitionCandidate(state RaftState) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	Debug(logTrace, "S%d in transitionCandidate", rf.me)

	if rf.state != state {
		Debug(logWarn, "S%d not same as server in transition %v", rf.me, state)
	}

	rf.currentTerm++
	rf.state = Candidate
	rf.votedFor = rf.me
	rf.voteTotal = 1

	Debug(logTrace, "S%d broadcasting to request votes", rf.me)
	// rf.broadcastRequestVote()
	args := RequestVoteArgs{
		Term:        rf.currentTerm,
		CandidateID: rf.me,
	}

	for s := range rf.peers {
		if s != rf.me {
			Debug(logTrace, "S%d sending vote to %d", rf.me, s)
			go rf.sendRequestVote(s, &args, &RequestVoteReply{})
		}
	}
}

// transitionLeader -> MUST be a Candidate that receives values from quorum of nodes.
func (rf *Raft) transitionLeader() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	Debug(logInfo, "S%d in transitionLeader()", rf.me)

	rf.state = Leader
	rf.sendHeartbeats()
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
	rf.sendVoteChan = make(chan bool)
	rf.winElectChan = make(chan bool)
	rf.transitionChan = make(chan bool)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	Debug(logDebug, "S%d Follower first initialized", rf.me)

	// start ticker goroutine to start elections
	go rf.ticker()

	// Hint 4?

	return rf
}
