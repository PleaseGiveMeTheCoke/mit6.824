package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

type Coordinator struct {
	FileName   []string
	taskFlag   []int
	reduceFlag []int
	remainTask int
	nReduce    int
	curPoint   int
	lock       sync.Mutex
	reduceNum  int
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) ReceiveMapReport(args *CompleteMapArgs, reply *CompleteReply) error {
	fileName := args.FileName
	taskNum := -1
	c.lock.Lock()
	for index, name := range c.FileName {
		if name == fileName {
			taskNum = index
			break
		}
	}
	if c.taskFlag[taskNum] == 1 {
		c.taskFlag[taskNum]++
	}
	c.lock.Unlock()
	return nil
}

func (c *Coordinator) ReceiveReduceReport(args *CompleteReduceArgs, reply *CompleteReply) error {
	c.lock.Lock()
	if c.reduceFlag[args.ReduceNum] == 1 {
		c.reduceFlag[args.ReduceNum]++
	}
	c.lock.Unlock()
	return nil
}

func (c *Coordinator) MapDone() bool {
	for _, flag := range c.taskFlag {
		if flag != 2 {
			return false
		}
	}
	return true
}
func (c *Coordinator) testCrash(index int) {
	if c.taskFlag[index] == 1 {
		fmt.Print("发现map任务超时 ： index = " + strconv.Itoa(index) + " fileName = " + c.FileName[index] + "\n")
		//超时
		c.taskFlag[index] = 0
	}
}

func (c *Coordinator) newFunc(index int) func() {
	return func() {
		c.testCrash(index)
	}
}

func (c *Coordinator) testCrash2(index int) {
	if c.reduceFlag[index] == 1 {
		fmt.Print("发现reduce任务超时 ： reduceNum = " + strconv.Itoa(index) + "\n")
		//超时
		c.reduceFlag[index] = 0
	}
}

func (c *Coordinator) newFunc2(index int) func() {
	return func() {
		c.testCrash2(index)
	}
}
func (c *Coordinator) ReceiveWorker(args *RpcArgs, reply *RpcReply) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	//map任务尚未分配完
	if c.curPoint < len(c.taskFlag) {
		// map
		reply.IsDone = false
		reply.IsMap = true
		reply.FileName = c.FileName[c.curPoint]
		reply.NReduce = c.nReduce
		f := c.newFunc(c.curPoint)
		time.AfterFunc(time.Second*10, f)
		//表示map任务已分配出去
		c.taskFlag[c.curPoint]++
		c.curPoint++

	} else {
		fmt.Print("map任务被分配完 查看有没有超时的map任务\n")
		for index, flag := range c.taskFlag {
			//查看有没有超时的map任务
			if flag == 0 {
				fmt.Print("发现超时的map任务，index = " + strconv.Itoa(index) + ", fileName = " + c.FileName[index] + "将其分配给该worker\n")

				reply.IsDone = false
				reply.IsMap = true
				reply.FileName = c.FileName[index]
				reply.NReduce = c.nReduce
				f := c.newFunc(index)
				time.AfterFunc(time.Second*10, f)
				//表示map任务已分配出去
				c.taskFlag[index]++
				return nil
			}
		}
		fmt.Print("未发现超时的map任务，等待map任务全部结束\n")
		c.lock.Unlock()
		if c.MapDone() == false {
			reply.IsDone = true
			c.lock.Lock()
			return nil
		}
		fmt.Print("map任务已全部结束，查看reduce任务\n")
		c.lock.Lock()
		//reduce
		if c.reduceNum < c.nReduce {
			fmt.Print("reduce任务尚未分配完，开始分配reduce任务\n")
			reply.IsDone = false
			reply.IsMap = false
			reply.ReduceNum = c.reduceNum
			//表示reduce任务已经分配出去
			c.reduceFlag[c.reduceNum]++
			f := c.newFunc2(c.reduceNum)
			time.AfterFunc(time.Second*10, f)
			c.reduceNum++
		} else {
			fmt.Print("reduce任务被分配完 查看有没有超时的reduce任务\n")
			for index, flag := range c.reduceFlag {

				if flag == 0 {
					fmt.Print("发现超时的reduce任务，index = " + strconv.Itoa(index) + ",将其分配给该worker\n")
					reply.IsDone = false
					reply.IsMap = false
					reply.ReduceNum = index
					c.reduceFlag[index]++
					f := c.newFunc2(index)
					time.AfterFunc(time.Second*10, f)
					return nil
				}
			}
			fmt.Print("暂未发现超时的reduce任务，暂不为worker分配任务\n")
			//all tasks done,don't need worker
			reply.IsDone = true
		}
	}
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	for _, flag := range c.reduceFlag {
		if flag != 2 {
			return false
		}
	}
	return true
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.remainTask = nReduce
	// Your code here.
	for _, filename := range files {
		c.FileName = append(c.FileName, filename)
		c.taskFlag = append(c.taskFlag, 0)
	}
	for i := 0; i < nReduce; i++ {
		c.reduceFlag = append(c.reduceFlag, 0)
	}
	c.nReduce = nReduce
	c.curPoint = 0
	c.server()
	return &c
}
