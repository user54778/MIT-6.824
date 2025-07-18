// Package Raft implements the Raft consensus algorithm for managing a replicated log.
package raft

import (
	"math/rand"
	"time"
)

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
	for !rf.killed() {
		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()
		switch state {
		case Follower:
			select {
			case event := <-rf.eventChannel:
				switch event {
				case HeartbeatEvent:
					Debug(logDebug, "S%d follower received heartbeat", rf.me)
				case VoteEvent:
					// timer is reset
					Debug(logDebug, "S%d follower received vote event notification", rf.me)
				}
			case <-time.After(time.Duration(rf.electionTimeout()) * time.Millisecond):
				Debug(logInfo, "S%d timed out. Calling transitionCandidate()", rf.me)
				rf.transitionCandidate(Follower)
			}
		case Candidate:
			select {
			case event := <-rf.eventChannel:
				switch event {
				case ElectionEvent:
					Debug(logDebug, "S%d candidate new Leader", rf.me)
					rf.transitionLeader()
				case TranistionToFollowerEvent:
					Debug(logDebug, "S%d candidate transition", rf.me)
				}
			case <-time.After(time.Duration(rf.electionTimeout()) * time.Millisecond):
				Debug(logInfo, "S%d timed out. Calling transitionCandidate() in Candidate", rf.me)
				rf.transitionCandidate(Candidate)
			}
		case Leader:
			select {
			case event := <-rf.eventChannel:
				switch event {
				case TranistionToFollowerEvent:
					Debug(logDebug, "S%d leader transition", rf.me)
				}
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
		rf.nonblockChanSend(rf.eventChannel, TranistionToFollowerEvent)
	}
}

// transitionCandidate -> MUST be a Follower that has timed out, suspecting the Leader failed.
func (rf *Raft) transitionCandidate(state RaftState) {
	// NOTE: This function is what performs leader election.
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
