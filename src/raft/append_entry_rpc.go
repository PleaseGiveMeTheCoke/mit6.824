package raft

type AppendEntryArgs struct {
	IsNull bool
	//leader’s term
	Term int
	//so follower can redirect clients
	LeaderId int
	//index of log entry immediately preceding new ones
	PrevLogIndex int
	//term of prevLogIndex entry
	PrevLogTerm int
	//leader’s commitIndex
	LeaderCommit int
	//log Entries to store (empty for heartbeat;may send more than one for efficiency)
	Entries []LogEntry
}

type AppendEntryReply struct {
	IsOpt     bool
	ConfTerm  int
	ConfIndex int
	Term      int
	Success   bool
	IsInstall bool
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	if rf.killed() {
		return false
	}
	rf.RaftPrint("sendAppendEntries to" + RaftToString(server))
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.RaftPrintB("Get AppendEntries from leader.leader id = " + RaftToString(args.LeaderId))
	reply.Term = rf.currentTerm
	//Reply false if term < currentTerm
	if rf.currentTerm > args.Term {
		rf.RaftPrintB("Invalid because of args.term < currentTerm,discard it and reply false")
		reply.Success = false
		return
	}
	rf.RaftPrintB("Heartbeat from Valid leader. leader id = " + RaftToString(args.LeaderId))
	rf.heartsbeats = 1
	if args.Term > rf.currentTerm {
		rf.RaftPrintB("AppendEntryRequest's term bigger than mine, convert to follower")
		rf.currentTerm = args.Term
		rf.currentState = 0
		rf.votedFor = -1
		rf.persist()
	}
	if rf.currentState == 1 {

		rf.RaftPrintB("I am a candidate, but I receive a AppendEntryRequest, so there has been a leader. convert to follower")
		rf.currentState = 0
		rf.votedFor = -1
		rf.persist()
	}
	if len(args.Entries) == 0 {
		rf.RaftPrintB("Empty entries,treat it as a hearbeat")
	}
	rf.RaftPrintf("Entries to append : %v", args.Entries)
	rf.RaftPrintB("PrevLogIndex : " + RaftToString(args.PrevLogIndex))
	//Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
	pli := args.PrevLogIndex
	if pli > 0 && (pli > rf.getLastLogIndex() || rf.getLogTerm(pli) != args.PrevLogTerm) {
		rf.RaftPrintB("My log doesn’t contain an entry at prevLogIndex " + RaftToString(pli) + " whose term matches prevLogTerm,reply false")
		rf.RaftPrintf("My log : %v\n", rf.log)
		reply.Success = false
		if pli <= rf.getLastLogIndex() {
			reply.IsOpt = true
			// if rf.getLastLogIndex() == 0 {
			// 	reply.ConfTerm = -1
			// } else {
			//reply.ConfTerm = rf.getLogTerm(rf.getLastLogIndex())
			//}
			// confIndex := pli
			// for confIndex > rf.lastIncludedIndex && rf.getLogTerm(confIndex) == rf.getLogTerm(pli) {
			// 	confIndex--
			// }
			// confIndex++
			// reply.ConfIndex = confIndex
			reply.ConfTerm = rf.getLogTerm(pli)
			confIndex := pli
			for confIndex > rf.lastIncludedIndex && rf.getLogTerm(confIndex) == rf.getLogTerm(pli) {
				confIndex--
			}
			reply.ConfIndex = confIndex+1
			rf.RaftPrintB("Opt!!,reply.confIndex = " + RaftToString(reply.ConfIndex) + " reply.confTerm = " + RaftToString(reply.ConfTerm))
		} else {
			reply.IsOpt = true
			//pli比我的最后一个Index大,所以我没有对应的日志,case3.
			reply.ConfTerm = -1
			reply.ConfIndex = rf.getLastLogIndex()
			// if rf.getLastLogIndex() == 0 {
			// 	reply.ConfTerm = -1
			// } else {
			// 	reply.ConfTerm = rf.getLogTerm(rf.getLastLogIndex())
			// }
			// confIndex := rf.getLastLogIndex()
			// for confIndex > rf.lastIncludedIndex && rf.getLogTerm(confIndex) == reply.ConfTerm {
			// 	confIndex--
			// }
			// confIndex++
			// reply.ConfIndex = confIndex
			rf.RaftPrintB("Opt!!,reply.confIndex = " + RaftToString(reply.ConfIndex) + " reply.confTerm = " + RaftToString(reply.ConfTerm))
		}
		return
	}
	reply.Success = true
	// If an existing entry conflicts with a new one (same index but different terms),
	// delete the existing entry and all that follow it
	// Append any new entries not already in the log
	rf.RaftPrintf("Before Append : %v", rf.log)
	//rf.cutLogTo(pli + 1)
	rf.log = rf.sliceLogTo(pli)
	rf.log = append(rf.log, args.Entries...)
	rf.RaftPrintf("After Append : %v", rf.log)
	rf.persist()
	rf.matchIndex[rf.me] = rf.getLastLogIndex()
	rf.nextIndex[rf.me] = rf.getLastLogIndex() + 1

	//If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = Min(args.LeaderCommit, rf.getLastLogIndex())
	}

}
