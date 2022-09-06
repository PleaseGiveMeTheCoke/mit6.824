package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"

	"bytes"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type OutBoundError struct {
	Err error
}

func (e *OutBoundError) Error() string {
	return e.Err.Error()
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	//Persistent state on all servers
	currentTerm int
	votedFor    int
	log         []LogEntry
	//Volatile state on all servers:
	applyCh           chan ApplyMsg
	commitIndex       int
	lastApplied       int
	lastIncludedIndex int
	lastIncludedTerm  int
	snapShot          []byte
	/**
		0 follower
		1 candidate
		2 leader
	**/
	currentState int
	heartsbeats  int
	rpcTimeOut   int
	rpcRound     int
	//Volatile state on leaders:
	/*
		for each server, index of the next log entry
		to send to that server (initialized to leader
		last log index + 1)
	*/
	nextIndex []int

	/*
		for each server, index of highest log entry
		known to be replicated on server
		(initialized to 0, increases monotonically)
	*/
	matchIndex []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	term = rf.currentTerm
	if rf.currentState != 2 {
		isleader = false
	} else {
		isleader = true
	}
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	data := w.Bytes()
	rf.persister.SaveStateAndSnapshot(data, rf.snapShot)
	//rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		rf.votedFor = -1
		rf.lastIncludedTerm = -1
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var lastIncludedIndex int
	var lastIncludedTerm int
	var log []LogEntry
	//
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&log) != nil || d.Decode(&lastIncludedIndex) != nil || d.Decode(&lastIncludedTerm) != nil {
		rf.snapShot = data
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm
	}
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	rf.RaftPrintC("snap index :" + RaftToString(index) + " ,discard log from " + RaftToString(rf.lastIncludedIndex) + " to it")
	rf.lastIncludedTerm = rf.getLogTerm(index)
	for i := rf.lastIncludedIndex; i < index; i++ {
		rf.log = rf.log[1:]
	}
	rf.snapShot = snapshot
	rf.lastIncludedIndex = index
	rf.persist()
	rf.mu.Unlock()
}

func Min(x, y int) int {
	if x < y {
		return x
	}
	return y
}

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := rf.getLastLogIndex() + 1
	term := rf.currentTerm
	isLeader := true
	if rf.currentState != 2 {
		isLeader = false
		return index, term, isLeader
	}
	newEntry := LogEntry{}
	newEntry.Command = command
	newEntry.Term = rf.currentTerm
	rf.RaftPrintf("I am a leader, append Entry %v", newEntry)
	rf.RaftPrintf("Before Append : %v", rf.log)
	rf.log = append(rf.log, newEntry)
	rf.persist()
	rf.matchIndex[rf.me] = rf.getLastLogIndex()
	rf.nextIndex[rf.me] = rf.getLastLogIndex() + 1
	// Your code here (2B).
	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}
func (rf *Raft) electionTimeOut() {
	if rf.currentState == 1 {
		rf.RaftPrint("election time out")
		//超时
		rf.currentState = 0
	}
}

func (rf *Raft) newFunc() func() {
	return func() {
		rf.electionTimeOut()
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	rf.RaftPrint("ticker begin")
	for rf.killed() == false {
		sleepNum := rand.Intn(200) + 300
		time.Sleep(time.Millisecond * time.Duration(sleepNum))
		rf.mu.Lock()
		if rf.heartsbeats == 0 && rf.currentState != 2 {
			rf.RaftPrint("time out and convert to candidate")
			//time out and convert to candidate
			rf.currentState = 1
			//start election
			rf.currentTerm++
			rf.votedFor = rf.me
			rf.persist()
			arg := &RequestVoteArgs{}
			arg.CandidateId = rf.me
			arg.Term = rf.currentTerm
			arg.LastLogIndex = rf.getLastLogIndex()
			if rf.getLastLogIndex() == 0 {
				arg.LastLogTerm = -1
			} else {
				arg.LastLogTerm = rf.getLogTerm(rf.getLastLogIndex())
			}
			voteCount := 0
			remainCount := len(rf.peers)
			for i := 0; i < len(rf.peers); i++ {
				go func(x int, term int) {
					reply := RequestVoteReply{}
					ok := rf.sendRequestVote(x, arg, &reply)
					rf.mu.Lock()
					if rf.currentTerm > term {
						rf.RaftPrint("Expired voting results, discard")
						rf.mu.Unlock()
						return
					}
					if !ok {
						rf.RaftPrint("we can't get message from server id = " + strconv.Itoa(x))
						remainCount--
						rf.mu.Unlock()
						return
					}
					if reply.Term > rf.currentTerm {
						rf.RaftPrint("find someone's term bigger than mine, update my term and convert to follower")
						rf.currentState = 0
						rf.currentTerm = reply.Term
						rf.votedFor = -1
						rf.persist()
					} else if reply.VoteGranted {
						voteCount++
						if voteCount > len(rf.peers)/2 {
							rf.RaftPrint("find voteCount has beyound half")
							remainCount = 0
						}
					}
					remainCount--
					rf.mu.Unlock()
				}(i, rf.currentTerm)
			}
			rf.mu.Unlock()
			f := rf.newFunc()
			time.AfterFunc(time.Millisecond*200, f)
			for true {
				time.Sleep(time.Millisecond * 30)
				rf.mu.Lock()
				if remainCount <= 0 || rf.currentState != 1 {
					rf.mu.Unlock()
					break
				}
				rf.mu.Unlock()
			}
			rf.mu.Lock()
			rf.RaftPrint("I have got result of vote from all alive servers")
			//If votes received from majority of servers: become leader
			if rf.currentState == 1 && voteCount > len(rf.peers)/2 {
				rf.RaftPrint("get beyond half servers grand,convert to leader")
				rf.currentState = 2
				rf.nextIndex = make([]int, len(rf.peers))
				rf.matchIndex = make([]int, len(rf.peers))
				rf.matchIndex[rf.me] = rf.getLastLogIndex()
				rf.nextIndex[rf.me] = 1 + rf.getLastLogIndex()
				for i := 0; i < len(rf.peers); i++ {
					if i == rf.me {
						continue
					}
					rf.nextIndex[i] = rf.getLastLogIndex() + 1
					rf.matchIndex[i] = 0
				}
			}

		} else if rf.currentState == 2 {
			rf.RaftPrint("i am a leader. sleep again")
		} else {
			rf.RaftPrint("get heartbeat.sleep again")
			rf.heartsbeats = 0
		}
		rf.mu.Unlock()
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
	}
	rf.RaftPrint("ticker exist because of raft was killed")
}

func (rf *Raft) applyer() {
	for !rf.killed() {
		time.Sleep(50 * time.Millisecond)
		rf.RaftPrintC("test commit")
		if rf.killed() {
			return
		}
		// rf.mu.Lock()
		// tmpIndex := rf.lastApplied
		// commitIndex := rf.commitIndex
		// rf.mu.Unlock()
		for {
			rf.mu.Lock()
			if rf.lastApplied < rf.commitIndex {
				msg := ApplyMsg{}
				msg.Command = rf.getLog(rf.lastApplied + 1).Command
				msg.CommandIndex = rf.lastApplied + 1
				msg.CommandValid = true
				rf.mu.Unlock()
				rf.applyCh <- msg
				rf.mu.Lock()
				if rf.lastApplied < msg.CommandIndex {
					rf.lastApplied = msg.CommandIndex
				}
				rf.mu.Unlock()
			} else {
				rf.mu.Unlock()
				break
			}
		}
		// for i := tmpIndex + 1; i <= commitIndex; i++ {
		// 	msg := ApplyMsg{}
		// 	rf.mu.Lock()
		// 	msg.Command = rf.getLog(i).Command
		// 	rf.mu.Unlock()
		// 	msg.CommandIndex = i
		// 	msg.CommandValid = true
		// 	rf.RaftPrintC("Before Apply")
		// 	rf.applyCh <- msg
		// 	rf.RaftPrintf("Apply one commited msg: index = "+RaftToString(i)+", command = %v", msg.Command)
		// }
		// rf.mu.Lock()
		// rf.lastApplied = rf.commitIndex
		// rf.mu.Unlock()
	}
}
func (rf *Raft) worker() {
	rf.RaftPrint("worker begin")
	for rf.killed() == false {
		time.Sleep(50 * time.Millisecond)
		if rf.killed() {
			return
		}
		if rf.currentState == 2 {
			rf.mu.Lock()
			// If there exists an N such that N > commitIndex, a majority
			// of matchIndex[i] ≥ N, and log[N].term == currentTerm:
			// set commitIndex = N
			rf.RaftPrintB("Start commitUpdate test")
			//tmpIndex := rf.commitIndex
			rf.RaftPrintC("Before commitUpdate test: commitIndex = " + RaftToString(rf.commitIndex))
			for i := rf.commitIndex + 1; i <= rf.getLastLogIndex(); i++ {
				if rf.getLogTerm(i) == rf.currentTerm {
					count := 0
					for index, value := range rf.matchIndex {
						if index == rf.me || value >= i {
							count++
						}
					}
					if count > len(rf.matchIndex)/2 {
						rf.commitIndex = i
					} else {
						break
					}
				}
			}
			rf.RaftPrintC("After commitUpdate test: commitIndex = " + RaftToString(rf.commitIndex))
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) sendRpc() {
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(x int, round int) {
			fastSend := false
			for !rf.killed() {
				if !fastSend {
					time.Sleep(50 * time.Millisecond)
				}
				if rf.currentState == 2 {
					rf.mu.Lock()
					args := AppendEntryArgs{}
					args.Term = rf.currentTerm
					args.LeaderId = rf.me
					args.PrevLogIndex = rf.nextIndex[x] - 1
					oldLen := rf.getLastLogIndex()
					if args.PrevLogIndex-1 < 0 {
						args.PrevLogTerm = -1
					} else {
						if args.PrevLogIndex < rf.lastIncludedIndex {
							rf.RaftPrintC("send InstallSnapShot RPC to server" + RaftToString(x) + "becasue of needed preIndex less than my lastIncludedINdex")
							oldLastInclude := rf.lastIncludedIndex
							installArgs := InstallSnapshotArgs{}
							installReply := InstallSnapshotReply{}
							installArgs.LastIncludedIndex = rf.lastIncludedIndex
							installArgs.LastIncludedTerm = rf.lastIncludedTerm
							installArgs.LeaderId = rf.me
							installArgs.SnapShot = rf.snapShot
							installArgs.Term = rf.currentTerm
							rf.mu.Unlock()
							ok := rf.sendInstallSnapshot(x, &installArgs, &installReply)
							if ok {
								rf.mu.Lock()
								if installReply.Term > rf.currentTerm {
									rf.RaftPrintC("Find someone's term bigger than mine, update my term and convert to follower")
									rf.currentState = 0
									rf.currentTerm = installReply.Term
									rf.votedFor = -1
									rf.persist()
									rf.mu.Unlock()
								} else {
									rf.RaftPrintC("Install to server" + RaftToString(x) + " success!")
									if rf.matchIndex[x] < oldLastInclude {
										rf.matchIndex[x] = oldLastInclude
									}
									rf.nextIndex[x] = oldLastInclude + 1
									rf.mu.Unlock()
								}
							} else {
								rf.RaftPrintC("Install to server" + RaftToString(x) + " fail,try again")
							}
							continue

						}
						args.PrevLogTerm = rf.getLogTerm(args.PrevLogIndex)

					}
					nextIndex := rf.nextIndex[x]
					for j := nextIndex; j <= rf.getLastLogIndex(); j++ {
						args.Entries = append(args.Entries, rf.getLog(j))
					}
					args.LeaderCommit = rf.commitIndex
					reply := AppendEntryReply{}
					rf.RaftPrintB("Send appendEntry rpc to peer" + strconv.Itoa(x))
					rf.mu.Unlock()
					ok := rf.sendAppendEntries(x, &args, &reply)
					if ok {
						//If AppendEntries fails because of log inconsistency:
						//decrement nextIndex and retry
						rf.mu.Lock()
						if !reply.Success {
							rf.RaftPrintB("Get response FAIL from peer" + strconv.Itoa(x))
							if reply.Term > rf.currentTerm {
								rf.RaftPrintB("Find someone's term bigger than mine, update my term and convert to follower")
								rf.currentState = 0
								rf.currentTerm = reply.Term
								rf.votedFor = -1
								fastSend = false
								rf.persist()
								rf.mu.Unlock()
							} else {
								rf.RaftPrintB("Decrement nextIndex[" + RaftToString(x) + "] and retry")
								rf.RaftPrintD("Before Decrement: " + RaftToString(rf.nextIndex[x]))
								if rf.nextIndex[x] > args.PrevLogIndex {
									rf.nextIndex[x] = args.PrevLogIndex
								}
								if rf.nextIndex[x] < 1 {
									rf.nextIndex[x] = 1
								}
								if reply.IsOpt {

									if reply.ConfIndex <= rf.lastIncludedIndex {
										rf.RaftPrintC("send InstallSnapShot RPC to server" + RaftToString(x) + " because of opt need")
										oldLastInclude := rf.lastIncludedIndex
										installArgs := InstallSnapshotArgs{}
										installReply := InstallSnapshotReply{}
										installArgs.LastIncludedIndex = rf.lastIncludedIndex
										installArgs.LastIncludedTerm = rf.lastIncludedTerm
										installArgs.LeaderId = rf.me
										installArgs.SnapShot = rf.snapShot
										installArgs.Term = rf.currentTerm
										rf.mu.Unlock()
										ok := rf.sendInstallSnapshot(x, &installArgs, &installReply)
										if ok {
											rf.mu.Lock()
											if installReply.Term > rf.currentTerm {
												rf.RaftPrintC("Find someone's term bigger than mine, update my term and convert to follower")
												rf.currentState = 0
												rf.currentTerm = installReply.Term
												rf.votedFor = -1
												rf.persist()
												rf.mu.Unlock()
											} else {
												rf.RaftPrintC("Install success!")
												if rf.matchIndex[x] < oldLastInclude {
													rf.matchIndex[x] = oldLastInclude
												}
												rf.nextIndex[x] = oldLastInclude + 1
												rf.mu.Unlock()
											}
										} else {
											rf.RaftPrintC("Install fail,try again")
										}
										continue
									}
									//case3: follower is too short
									if reply.ConfTerm == -1 {
										rf.nextIndex[x] = reply.ConfIndex
									} else {
										begin, end := rf.haveTerm(reply.ConfTerm)
										if begin == -1 {
											//case 1:leader don't have confTerm
											rf.nextIndex[x] = reply.ConfIndex
										} else {
											//case 2:leader do have confTerm
											rf.nextIndex[x] = end
										}
									}
									// if reply.ConfTerm == -1 {
									// 	//follower has no Log
									// 	rf.nextIndex[x] = reply.ConfIndex
									// }
									// if rf.getLogTerm(reply.ConfIndex) == reply.ConfTerm {
									// 	rf.nextIndex[x] = reply.ConfIndex + 1
									// } else {
									// 	rf.nextIndex[x] = reply.ConfIndex
									// }
									rf.RaftPrintB("Optmization ! new nextIndex of peer" + strconv.Itoa(x) + " is" + strconv.Itoa(rf.nextIndex[x]))

								}
								fastSend = true
								rf.mu.Unlock()
							}
							// If successful: update nextIndex and matchIndex for follower
						} else {
							rf.RaftPrintB("Get response SUCCESS from peer" + strconv.Itoa(x))
							// If successful: update nextIndex and matchIndex for follower
							if rf.matchIndex[x] < oldLen {
								rf.matchIndex[x] = oldLen
							}
							rf.nextIndex[x] = oldLen + 1

							// rf.nextIndex[x] = oldLen + 1
							// rf.matchIndex[x] = oldLen
							fastSend = false
							rf.mu.Unlock()
						}
					} else {
						fastSend = false
						rf.RaftPrintB("Can't connect to peer" + strconv.Itoa(x) + ", retry")
					}
				}
			}
		}(i, rf.rpcRound)
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	// Your initialization code here (2A, 2B, 2C).
	rf.currentState = 0
	rf.heartsbeats = 0
	rf.rpcTimeOut = 0
	rf.rpcRound = 0
	rf.applyCh = applyCh
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.readPersist(persister.ReadSnapshot())
	if rf.lastIncludedIndex == 0 {
		rf.lastIncludedTerm = -1
	}
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = rf.getLastLogIndex() + 1
		rf.matchIndex[i] = 0
	}
	rf.lastApplied = rf.lastIncludedIndex
	rf.commitIndex = rf.lastIncludedIndex
	// start ticker goroutine to start elections
	//rf.votedFor = -1
	go rf.ticker()
	go rf.applyer()
	go rf.worker()
	// for i := 0; i < len(rf.peers); i++ {
	// 	if i != rf.me {
	// 		go sendHeartBeat(rf, i)
	// 	}
	// }
	// rf.sendRpc2()
	rf.sendRpc()
	return rf
}
