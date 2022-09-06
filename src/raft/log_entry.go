package raft

type LogEntry struct {
	Term    int
	Command interface{}
}

func (rf *Raft) getLog(index int) LogEntry {
	return rf.log[index-rf.lastIncludedIndex-1]
}

func (rf *Raft) getLogTerm(index int) int {
	if index == rf.lastIncludedIndex {
		return rf.lastIncludedTerm
	}
	return rf.log[index-rf.lastIncludedIndex-1].Term
}

//slice log from end to front
func (rf *Raft) sliceLogTo(index int) []LogEntry {
	return rf.log[:index-rf.lastIncludedIndex]
}

func (rf *Raft) getLastLogIndex() int {
	return len(rf.log) + rf.lastIncludedIndex
}

func (rf *Raft) haveTerm(term int) (int, int) {
	begin := -1
	end := -1
	flag := false
	for index, entry := range rf.log {
		if begin == -1 && entry.Term == term {
			begin = index
			end = index
		}
		if end != -1 && entry.Term != term {
			end = index - 1
			flag = true
			break
		}
	}
	if !flag {
		end = len(rf.log) - 1
	}
	if begin == -1 {
		return -1, -1
	}
	return begin + rf.lastIncludedIndex + 1, end + rf.lastIncludedIndex + 1

}

//cut log from front to end
func (rf *Raft) cutLogTo(index int) {
	for i := rf.lastIncludedIndex + 1; i < index; i++ {
		rf.log = rf.log[1:]
	}
}
