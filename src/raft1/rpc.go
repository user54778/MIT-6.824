package raft

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
