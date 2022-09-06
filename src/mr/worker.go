package mr

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	//id := time.Now().Unix
	for true {
		//请求任务
		reply := CallRpc()
		if reply == nil {
			//fmt.Print("Worker Id :" + strconv.Itoa(int(id())) + "exit because of coordinator has exit\n")
			return
		}
		if reply.IsDone {
			time.Sleep(time.Second)
			continue
		}

		if reply.IsMap {
			//map任务

			filename := reply.FileName
			//fmt.Print("Worker Id :" + strconv.Itoa(int(id())) + "get Task Map,fileName = " + reply.FileName + "\n")
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("cannot open %v", filename)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", filename)
			}
			file.Close()
			kva := mapf(filename, string(content))
			random := uuid.New().String()
			for _, kv := range kva {
				bucketNum := ihash(kv.Key) % reply.NReduce
				//midFile, err :=os.Create("mr-mid-"+strconv.Itoa(bucketNum))
				//arry := strings.Split(reply.FileName, "/")
				//"mr-"+arry[len(arry)-1]+"-"+strconv.Itoa(bucketNum)
				midFile, err := os.OpenFile(random+strconv.Itoa(bucketNum), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
				if err != nil {
					log.Fatalf("cannot write %v because of %v ", "mr-"+reply.FileName+"-"+strconv.Itoa(bucketNum), err)
				}
				writer := bufio.NewWriter(midFile)
				writer.WriteString(kv.Key + " " + kv.Value + "\n")
				writer.Flush()
			}
			arry := strings.Split(reply.FileName, "/")
			for i := 0; i < reply.NReduce; i++ {
				os.Rename(random+strconv.Itoa(i), "mr-"+arry[len(arry)-1]+"-"+strconv.Itoa(i))
			}
			//汇报完成
			ok := ReportCompleteMap(filename)
			if ok == nil {
				fmt.Printf("call ReportCompleteMap failed!\n")
				return
			}
		} else {
			//reduce 任务
			//fmt.Print("Worker Id :" + strconv.Itoa(int(id())) + "get Task Reduce,reduceNum = " + strconv.Itoa(reply.ReduceNum) + "\n")
			reduceNum := reply.ReduceNum
			var files []string
			//遍历目录
			filepath.Walk(".",
				func(path string, f os.FileInfo, err error) error {
					if f == nil {
						return err
					}
					if f.IsDir() {
						return nil
					}
					if strings.HasSuffix(f.Name(), strconv.Itoa(reduceNum)) {
						fmt.Print("for debug\n")
						arry := strings.Split(f.Name(), "/")
						files = append(files, arry[len(arry)-1])
					}
					return nil
				})
			intermediates := []KeyValue{}
			for _, midFileName := range files {
				f, err := os.OpenFile(midFileName, os.O_CREATE|os.O_RDONLY, 0666)
				if err != nil {
					log.Fatalf("cannot read %v", midFileName)
				}
				buf := bufio.NewReader(f)
				intermediate := []KeyValue{}
				for true {
					line, err := buf.ReadString('\n')
					if err != nil {
						if err == io.EOF {
							break
						}
						log.Fatalf("read error :%v", midFileName)
					}
					line = strings.TrimSpace(line)
					kv := strings.Split(line, " ")
					Kv := KeyValue{}
					Kv.Key = kv[0]
					Kv.Value = kv[1]
					intermediate = append(intermediate, Kv)
				}
				intermediates = append(intermediates, intermediate...)
			}

			sort.Sort(ByKey(intermediates))
			//oname := "mr-out-" + strconv.Itoa(reduceNum)
			ofile, err2 := os.CreateTemp(".", "213")
			//ofile, _ := os.Create(oname)
			if err2 != nil {
				log.Fatalf("open tempFile error")
			}
			i := 0
			for i < len(intermediates) {
				j := i + 1
				for j < len(intermediates) && intermediates[j].Key == intermediates[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, intermediates[k].Value)
				}
				output := reducef(intermediates[i].Key, values)

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(ofile, "%v %v\n", intermediates[i].Key, output)

				i = j
			}
			ofile.Close()
			oname := "mr-out-" + strconv.Itoa(reduceNum)
			os.Rename(ofile.Name(), oname)
			//汇报完成
			ok := ReportCompleteReduce(reduceNum)
			if ok == nil {
				fmt.Printf("call ReportCompleteReduce failed!\n")
				return
			}
		}
	}
}
func ReportCompleteReduce(reduceNum int) *CompleteReply {
	args := CompleteReduceArgs{}
	args.ReduceNum = reduceNum
	reply := CompleteReply{}
	ok := call("Coordinator.ReceiveReduceReport", &args, &reply)

	if ok {
		return &reply
	} else {
		fmt.Printf("call failed!\n")
		return nil
	}
}
func ReportCompleteMap(fileName string) *CompleteReply {
	args := CompleteMapArgs{}

	args.FileName = fileName

	reply := CompleteReply{}

	ok := call("Coordinator.ReceiveMapReport", &args, &reply)

	if ok {
		return &reply
	} else {
		fmt.Printf("call failed!\n")
		return nil
	}
}
func CallRpc() *RpcReply {
	args := RpcArgs{}

	args.Id = ihash(strconv.FormatInt(time.Now().Unix(), 10))

	reply := RpcReply{}

	ok := call("Coordinator.ReceiveWorker", &args, &reply)

	if ok {
		return &reply
	} else {
		fmt.Printf("call failed!\n")
		return nil
	}
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
