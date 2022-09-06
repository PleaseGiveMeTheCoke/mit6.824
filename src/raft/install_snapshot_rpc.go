package raft

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	SnapShot          []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	if rf.killed() {
		return false
	}
	rf.RaftPrint("sendInstallSnapshot to " + RaftToString(server))
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}
	if args.LastIncludedIndex <= rf.lastIncludedIndex {
		rf.RaftPrintD("Expired install rpc request,discard")
		return
	}
	rf.RaftPrintD("Get Install request")
	rf.snapShot = args.SnapShot
	msg := ApplyMsg{}
	msg.SnapshotValid = true
	msg.SnapshotIndex = args.LastIncludedIndex
	msg.SnapshotTerm = args.LastIncludedTerm
	msg.Snapshot = args.SnapShot

	if rf.commitIndex < args.LastIncludedIndex {
		rf.commitIndex = args.LastIncludedIndex
	}
	if rf.lastApplied < args.LastIncludedIndex {
		rf.lastApplied = args.LastIncludedIndex
	}

	if rf.getLastLogIndex() >= args.LastIncludedIndex {
		if rf.getLogTerm(args.LastIncludedIndex) == args.LastIncludedTerm {
			rf.cutLogTo(args.LastIncludedIndex + 1)
			rf.lastIncludedIndex = args.LastIncludedIndex
			rf.lastIncludedTerm = args.LastIncludedTerm
			rf.applyCh <- msg
			rf.persist()
			return
		}
	}
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	//index-rf.startIndex
	rf.log = []LogEntry{}
	rf.applyCh <- msg
	rf.persist()
}
