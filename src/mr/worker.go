package mr

import (
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

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

func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	for true {
		worker(mapf, reducef)
		time.Sleep(time.Second / 10)
	}
}

//
// main/mrworker.go calls this function.
//
func worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	args := TaskGetArgs{}

	nReduce := NReduce{}
	callGetNReduce(&args, &nReduce)

	reply := TaskReply{}
	callGetTask(&args, &reply)

	// read file here
	if reply.ReduceTaskIdx != -1 {
		//println("finish all file process")
		// do reduce
		filename := reply.ReduceFileName
		content := readFile(filename)

		// group content by key
		arr := strings.Split(content, "\n")
		//println("len", len(arr))
		var kvs [][]string

		for _, v := range arr {
			//println("word:", v)
			if v == "" {
				continue
			}
			kv := strings.Split(v, " ")
			kvs = append(kvs, []string{kv[0], kv[1]})
		}

		kvsMap := map[string][]string{}
		for _, v := range kvs {
			kvsMap[v[0]] = append(kvsMap[v[0]], v[1])
		}

		oname := "mr-out-0"
		for s, v := range kvsMap {
			ss := reducef(s, v)
			writeFile(oname, s+" "+ss+"\n")
			//println("KV: ", s, " ", ss)
		}

		callFinishMapTask(&TaskGetArgs{
			Idx: reply.ReduceTaskIdx,
		}, &TaskFinishReply{})

	} else if reply.MapTaskIdx != -1 {
		println("ready to read: ", reply.MapFilename)
		filename := reply.MapFilename

		// read file
		content := readFile(filename)

		// do map
		res := mapf(filename, content)

		// write kv into a tmp file
		for _, v := range res {
			taskNum := ihash(v.Key) % nReduce.N
			writeFile(strconv.Itoa(taskNum)+".txt", v.Key+" "+v.Value+"\n")
		}

		// send reduce task back to master
		callFinishMapTask(&TaskGetArgs{
			Idx: reply.MapTaskIdx,
		}, &TaskFinishReply{})
	}

}

func readFile(filename string) string {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	return string(content)
}

func writeFile(filename string, ctx string) {
	//// delete if exist
	//e, _ := exists(filename)
	//if e {
	//	err := os.Remove(filename)
	//	if err != nil {
	//		log.Fatalf("can't delete %s,  %v \n", filename, err)
	//	}
	//}

	// create and open
	file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}

	_, err = file.Write([]byte(ctx))
	if err != nil {
		log.Fatalf("can't write %s,  %v \n", filename, err)
	}
	file.Close()
}

func exists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return true, err
}

func callGetTask(args *TaskGetArgs, reply *TaskReply) {
	ok := call("Coordinator.GetTask", args, reply)
	if ok {
		fmt.Printf("Get Task %d %d\n", reply.MapTaskIdx, reply.ReduceTaskIdx)
	} else {
		fmt.Printf("call failed!\n")
	}
}

func callGetNReduce(args *TaskGetArgs, reply *NReduce) {
	ok := call("Coordinator.GetNReduce", args, reply)
	if ok {
		fmt.Printf("NReduce %d\n", reply.N)
	} else {
		fmt.Printf("call failed!\n")
	}
}

func callFinishMapTask(args *TaskGetArgs, reply *TaskFinishReply) {
	ok := call("Coordinator.FinishMapTask", args, reply)
	if ok {
		fmt.Printf("Finish Map Task:  %d\n", args.Idx)
	} else {
		fmt.Printf("call failed!\n")
	}
}

func callFinishReduceTask(args *TaskGetArgs, reply *TaskFinishReply) {
	ok := call("Coordinator.FinishReduceTask", args, reply)
	if ok {
		fmt.Printf("Finish Map Task:  %d\n", args.Idx)
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
