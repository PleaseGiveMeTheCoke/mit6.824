package raft

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	if rf.killed() {
		return false
	}
	rf.RaftPrint("sendRequestVote to" + RaftToString(server))
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {

	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.RaftPrint("get heartbeat from candidate. candidate id = " + RaftToString(args.CandidateId))
	rf.heartsbeats = 1
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		rf.RaftPrint("VoteRequest's term less than mine, discard and granted false for request from raft id = " + RaftToString(args.CandidateId))
		reply.VoteGranted = false
		return
	}
	if args.Term > rf.currentTerm {
		rf.RaftPrint("VoteRequest's term bigger than mine, convert to follower")
		rf.currentTerm = args.Term
		rf.currentState = 0
		rf.votedFor = -1
		rf.persist()
	}
	moreUpToDate := false
	if rf.getLastLogIndex() == 0 {
		moreUpToDate = true
	} else {
		myLastTerm := rf.getLogTerm(rf.getLastLogIndex())
		moreUpToDate = args.LastLogTerm > myLastTerm || (args.LastLogTerm == myLastTerm && args.LastLogIndex >= rf.getLastLogIndex())
	}
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && moreUpToDate {
		rf.RaftPrint("Granted true for request from raft id = " + RaftToString(args.CandidateId))
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.persist()
		return
	}
	rf.RaftPrint("Granted false for request from raft id = " + RaftToString(args.CandidateId))
	reply.VoteGranted = false
}
