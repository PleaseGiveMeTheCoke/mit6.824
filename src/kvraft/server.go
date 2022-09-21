package kvraft

import (
	"bytes"
	"fmt"
	"log"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = false

var printFlag = 2

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

	//Put,Append or Get
	Meth    string
	Key     string
	Value   string
	Version string
	LastVersion string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	table      map[string]string
	versionToIndex        map[string]int
	applyIndex int
	indexToVersion map[int]string
}

func ToString(a int) string {
	return strconv.Itoa(a)
}
func (kv *KVServer) ServerPrint(msg string) {
	if printFlag == 1  && !kv.killed() {
		fmt.Println("Server " + ToString(kv.me) + " :" + msg)
	}
}
func (kv *KVServer) ServerPrint2(msg string) {
	if 2 == 1  && !kv.killed() {
		fmt.Println("Server " + ToString(kv.me) + " :" + msg)
	}
}

func (kv *KVServer) ServerPrintf(msg string, a interface{}) {
	if printFlag == 1 {
		fmt.Printf("Server "+ToString(kv.me)+" :"+msg+"\n", a)
	}
}
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.ServerPrint("Handle Get Rpc")
	op := Op{}
	op.Meth = "Get"
	op.Key = args.Key
	op.Version = args.Version
	op.LastVersion = args.LastVersion
	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		kv.ServerPrint("I am not a leader, reply ErrWrongLeader")
		reply.Err = ErrWrongLeader
		return
	} else {
		kv.ServerPrint("I am a leader, wait for commandIndex = " + ToString(index))
		for {
			kv.mu.Lock()
			if term != kv.rf.GetCurrentTerm(){
				kv.ServerPrint("Leader changed, reply ErrWrongLeader")
				reply.Err = ErrWrongLeader
				kv.mu.Unlock()
				return
			}
			if index <= kv.applyIndex {
				if kv.indexToVersion[index] != op.Version {
					kv.ServerPrint("Leader changed, reply ErrWrongLeader")
					reply.Err = ErrWrongLeader
					kv.mu.Unlock()
					return
				}
				delete(kv.indexToVersion,index)
				kv.ServerPrint("ApplyIndex = " + ToString(kv.applyIndex) + ",bigger than commandIndex")
				reply.Err = OK
				reply.Value = kv.table[op.Key]
				kv.ServerPrint("===Get Key:" + args.Key + " Result: " + reply.Value)
				kv.mu.Unlock()
				return
			} else {
				kv.ServerPrint("ApplyIndex = " + ToString(kv.applyIndex) + ",smaller than commandIndex, sleep and try again")
				kv.mu.Unlock()
				time.Sleep(50 * time.Millisecond)
			}
		}
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.ServerPrint("Handle PutAppend Rpc")
	op := Op{}
	op.Meth = args.Op
	op.Key = args.Key
	op.Value = args.Value
	op.Version = args.Version
	op.LastVersion = args.LastVersion
	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		kv.ServerPrint("I am not a leader, reply ErrWrongLeader")
		reply.Err = ErrWrongLeader
		return
	} else {
		kv.ServerPrint("I am a leader, wait for commandIndex = " + ToString(index))
		for {
			kv.mu.Lock()
			if term != kv.rf.GetCurrentTerm(){
				kv.ServerPrint("Leader changed, reply ErrWrongLeader")
				reply.Err = ErrWrongLeader
				kv.mu.Unlock()
				return
			}
			if index <= kv.applyIndex {
				kv.ServerPrint("ApplyIndex = " + ToString(kv.applyIndex) + ",bigger than commandIndex")
				if kv.indexToVersion[index] != op.Version {
					kv.ServerPrint("Leader changed, reply ErrWrongLeader")
					reply.Err = ErrWrongLeader
					kv.mu.Unlock()
					return
				}
				kv.ServerPrint("PutAppend Success")
				if op.Meth == "Put" {
					kv.ServerPrint("===Put Key:" + args.Key + " Value: " + args.Value)
				} else {
					kv.ServerPrint("===Append Key:" + args.Key + " Value: " + args.Value)
				}
				reply.Err = OK
				kv.mu.Unlock()
				return
			} else {
				kv.ServerPrint("ApplyIndex = " + ToString(kv.applyIndex) + ",smaller than commandIndex, sleep and try again")
				kv.mu.Unlock()
				time.Sleep(50 * time.Millisecond)
			}
		}
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	// You may need initialization code here.
	snapshot := persister.ReadSnapshot()
	kv.readPersist(snapshot)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	
	go kv.listenToRaft()
	// You may need initialization code here.
	go kv.testSnapshot()
	return kv
}
func (kv *KVServer) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		kv.table = make(map[string]string)
		kv.versionToIndex = make(map[string]int)
		kv.indexToVersion = make(map[int]string)
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var table map[string]string
	var versionToIndex map[string]int
	var indexToVersion map[int]string
	var applyIndex int
	//
	if d.Decode(&table) != nil || d.Decode(&versionToIndex)!=nil || d.Decode(&indexToVersion)!=nil || d.Decode(&applyIndex)!=nil{
		kv.ServerPrint("Can't decode table")
		kv.table = make(map[string]string)
		kv.versionToIndex = make(map[string]int)
		kv.indexToVersion = make(map[int]string)
		kv.applyIndex = 0
	} else {
		kv.table = table
		kv.versionToIndex = versionToIndex
		kv.indexToVersion = indexToVersion
		kv.applyIndex = applyIndex
	}
}
func (kv *KVServer) testSnapshot() {
	if(kv.maxraftstate == -1){
		return
	}
	for {
		time.Sleep(100 * time.Millisecond)
		kv.mu.Lock()
		if kv.maxraftstate < kv.rf.GetPersister().RaftStateSize() {
			w := new(bytes.Buffer)
			e := labgob.NewEncoder(w)
			e.Encode(kv.table)
			e.Encode(kv.versionToIndex)
			e.Encode(kv.indexToVersion)
			e.Encode(kv.applyIndex)
			kv.ServerPrint2("Do a snapshot!! maxraft = "+ToString(kv.maxraftstate)+" and raft = "+ToString(kv.rf.GetPersister().RaftStateSize()))

			kv.rf.Snapshot(kv.applyIndex,w.Bytes())
			
		}
		kv.mu.Unlock()
	}
}
func (kv *KVServer) listenToRaft() {
	for {
		command := <-kv.applyCh
		if command.CommandValid {
			kv.mu.Lock()
			kv.ServerPrint("Get a Commited Command from raft")
			kv.ServerPrint("CommandIndex: " + ToString(command.CommandIndex))
			op := command.Command.(Op)
			kv.applyIndex = command.CommandIndex
			kv.indexToVersion[command.CommandIndex] = op.Version
			delete(kv.versionToIndex,op.LastVersion)
			_, ok := kv.versionToIndex[op.Version]
			if ok {
				kv.ServerPrint("Get a Commited Command that has been appllied,discard")
				kv.mu.Unlock()
				continue
			} else {
				kv.versionToIndex[op.Version] = 1
			}
			kv.ServerPrintf("Op: %v", op)
			if op.Meth == "Get" {
			} else if op.Meth == "Put" {
				kv.table[op.Key] = op.Value
			} else if op.Meth == "Append" {
				origin := kv.table[op.Key]
				kv.table[op.Key] = origin + op.Value
			}
			kv.mu.Unlock()
		}else{
			kv.mu.Lock()
			kv.ServerPrint("Get a Snapshot Command from raft")
			kv.applyIndex = command.SnapshotIndex
			kv.readPersist(command.Snapshot)
			kv.mu.Unlock()
		}
		time.Sleep(time.Millisecond*20)
	}
}
