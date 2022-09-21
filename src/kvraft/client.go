package kvraft

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"time"
	uuid "github.com/satori/go.uuid"
	"6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	preLeader int
	lastVersion string
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.preLeader = 0
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	ok := false
	args := GetArgs{}
	args.Key = key
	//args.Version = strconv.FormatInt(time.Now().UnixNano(), 10)
	args.Version = uuid.NewV4().String()
	args.LastVersion = ck.lastVersion
	reply := GetReply{}
	for true {

		ok = ck.sendGetRpc(ck.preLeader, &args, &reply)
		if !ok {
			ck.CkPrint("fail rpc and change server")
			ck.preLeader++
			if ck.preLeader == len(ck.servers) {
				ck.preLeader = 0
			}
			time.Sleep(10*time.Millisecond)
			continue
		}
		if reply.Err == OK {
			ck.lastVersion = args.Version
			return reply.Value
		}
		if reply.Err == ErrNoKey {
			ck.lastVersion = args.Version
			return ""
		}
		if reply.Err == ErrWrongLeader {
			ck.preLeader++
			if ck.preLeader == len(ck.servers) {
				ck.preLeader = 0
			}
		}
	}
	return ""
}
func (ck *Clerk) sendGetRpc(server int, args *GetArgs, reply *GetReply) bool {
	// You will have to modify this function.
	ok := ck.servers[server].Call("KVServer.Get", args, reply)
	return ok
}
func (ck *Clerk) sendPutAppendRpc(server int, args *PutAppendArgs, reply *PutAppendReply) bool {
	// You will have to modify this function.
	ok := ck.servers[server].Call("KVServer.PutAppend", args, reply)
	return ok
}
func (ck *Clerk) CkPrint(msg string) {
	// You will have to modify this function.
	if printFlag == 1 {
		fmt.Println(msg)
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs{}
	reply := PutAppendReply{}
	args.Key = key
	args.Value = value
	args.Op = op
	//args.Version = strconv.FormatInt(time.Now().UnixNano(), 10)
	args.Version = uuid.NewV4().String()
	args.LastVersion = ck.lastVersion

	//ck.CkPrint("Op Version: " + args.Version)
	for true {
		ok := ck.sendPutAppendRpc(ck.preLeader, &args, &reply)
		if !ok {
			ck.CkPrint("fail r	pc and change server")
			ck.preLeader++
			if ck.preLeader == len(ck.servers) {
				ck.preLeader = 0
			}
			time.Sleep(10*time.Millisecond)
			continue
		}
		if reply.Err == OK {
			ck.CkPrint("success!!")
			ck.lastVersion = args.Version
			return
		}
		if reply.Err == ErrWrongLeader {
			ck.CkPrint("wrong leader")
			ck.preLeader++
			if ck.preLeader == len(ck.servers) {
				ck.preLeader = 0
			}
		}
	}

}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
