package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//
type RpcArgs struct {
	Id int
}
type RpcReply struct {
	IsDone    bool
	IsMap     bool
	FileName  string
	ReduceNum int
	NReduce   int
}
type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type CompleteMapArgs struct {
	FileName string
}

type CompleteReduceArgs struct {
	ReduceNum int
}

type CompleteReply struct {
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
