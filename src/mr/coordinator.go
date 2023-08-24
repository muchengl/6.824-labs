package mr

import (
	"log"
	"strconv"
	"sync"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type mapTask struct {
	fileName string

	// 0 haven't been process
	// 1 finished process
	// -1 in process
	status int
}

type reduceTask struct {
	//fileName string

	// 0 haven't been process
	// 1 finished process
	// -1 in process
	status int
}

type Coordinator struct {
	// Your definitions here.
	mu          sync.RWMutex
	mapTasks    []*mapTask
	reduceTasks []*reduceTask
	nReduce     int
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) GetTask(args *TaskGetArgs, reply *TaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// find an unprocessed task
	for i, t := range c.mapTasks {
		if t.status == 0 {
			println("Find map task: ", i, " ", t.fileName)
			reply.MapTaskIdx = i
			reply.MapFilename = t.fileName

			reply.ReduceTaskIdx = -1

			t.status = -1
			return nil
		}
	}
	reply.MapTaskIdx = -1
	reply.MapFilename = ""

	// choose a reduce task
	for i, t := range c.reduceTasks {
		if t.status == 0 {
			println("Find reduce task: ", i)

			reply.ReduceTaskIdx = i
			reply.ReduceFileName = strconv.Itoa(i+1) + ".txt"

			t.status = -1
			return nil
		}
	}
	reply.ReduceTaskIdx = -1
	reply.ReduceFileName = ""

	return nil
}

func (c *Coordinator) GetNReduce(args *TaskGetArgs, reply *NReduce) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	println("nReduce:", c.nReduce)
	reply.N = c.nReduce
	return nil
}

func (c *Coordinator) FinishMapTask(args *TaskGetArgs, reply *TaskFinishReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.mapTasks[args.Idx].status = 1
	return nil
}

func (c *Coordinator) FinishReduceTask(args *TaskGetArgs, reply *TaskFinishReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.reduceTasks[args.Idx].status = 1
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
	c.mu.RLock()
	defer c.mu.RUnlock()

	for _, v := range c.mapTasks {
		if v.status != 1 {
			return false
		}
	}

	for _, v := range c.reduceTasks {
		if v.status != 1 {
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
	c := Coordinator{
		mapTasks:    make([]*mapTask, len(files)),
		reduceTasks: make([]*reduceTask, nReduce),
		nReduce:     nReduce,
	}

	for i := range c.mapTasks {
		println("Init File ", i, files[i])

		c.mapTasks[i] = &mapTask{
			fileName: files[i],
			status:   0,
		}
	}

	for i := range c.reduceTasks {
		c.reduceTasks[i] = &reduceTask{
			status: 0,
		}
	}

	c.server()
	return &c
}
