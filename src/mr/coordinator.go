package mr

import (
	"encoding/json"
	"log"
	"sort"
	"strings"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type task interface {
	getStatus() int
	setStatus(s int)
}

type mapTask struct {
	mu       sync.RWMutex
	fileName string

	// 0 haven't been process
	// 1 finished process
	// -1 in process
	status int
}

func (m *mapTask) getStatus() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.status
}

func (m *mapTask) setStatus(s int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.status = s
}

type reduceTask struct {
	mu        sync.RWMutex
	fileNames []string

	// 0 haven't been process
	// 1 finished process
	// -1 in process
	status int
}

func (m *reduceTask) getStatus() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.status
}

func (m *reduceTask) setStatus(s int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.status = s
}

type Coordinator struct {
	// Your definitions here.
	mu          sync.RWMutex
	mapTasks    []*mapTask
	reduceTasks []*reduceTask
	nReduce     int
	finalFiles  []string
	exit        bool
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) GetTask(args *TaskGetArgs, reply *TaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// find an unprocessed map task
	for i, t := range c.mapTasks {
		if t.status == 0 {
			//println("Find map task: ", i, " ", t.fileName)
			reply.MapTaskIdx = i
			reply.MapFilename = t.fileName

			reply.ReduceTaskIdx = -1

			t.status = -1

			go func() {
				time.Sleep(time.Second * 10)
				c.monitor(t)
			}()

			return nil
		}
	}
	reply.MapTaskIdx = -1
	reply.MapFilename = ""

	// make sure all map task finished
	for _, t := range c.mapTasks {
		if t.status != 1 {
			reply.ReduceTaskIdx = -1
			reply.ReduceFileNames = ""

			return nil
		}
	}

	// choose a reduce task
	for i, t := range c.reduceTasks {
		if t.status == 0 {
			//println("Find reduce task: ", i)
			reply.ReduceTaskIdx = i
			fns, _ := json.Marshal(t.fileNames)
			reply.ReduceFileNames = string(fns)

			t.status = -1

			go func() {
				time.Sleep(time.Second * 10)
				c.monitor(t)
			}()

			return nil
		}
	}
	reply.ReduceTaskIdx = -1
	reply.ReduceFileNames = ""

	return nil
}

func (c *Coordinator) monitor(t task) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if t.getStatus() != 1 {
		t.setStatus(0)
	}
}

func (c *Coordinator) GetNReduce(args *TaskGetArgs, reply *NReduce) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	reply.N = c.nReduce
	return nil
}

func (c *Coordinator) FinishMapTask(args *TaskGetArgs, reply *TaskFinishReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	fns := map[int]string{}
	json.Unmarshal([]byte(args.FileNames), &fns)
	for k, v := range fns {
		c.reduceTasks[k].fileNames = append(c.reduceTasks[k].fileNames, v)
	}

	c.mapTasks[args.Idx].status = 1

	return nil
}

func (c *Coordinator) FinishReduceTask(args *TaskGetArgs, reply *TaskFinishReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.reduceTasks[args.Idx].status = 1
	c.finalFiles = append(c.finalFiles, args.FileNames)

	return nil
}

func (c *Coordinator) CanExit(args *CanExitReq, reply *CanExitReply) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	reply.Flag = c.exit
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
	c.mu.Lock()
	defer c.mu.Unlock()

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

	// sort
	oname := "mr-wc-all"

	content := ""
	for i := 0; i < len(c.finalFiles); i++ {
		fn := c.finalFiles[i]
		content += readFile(fn)
	}

	// group content by key
	arr := strings.Split(content, "\n")
	var kvs [][]string

	for _, v := range arr {
		if v == "" {
			continue
		}
		kv := strings.Split(v, " ")
		kvs = append(kvs, []string{kv[0], kv[1]})
	}

	sort.Slice(kvs, func(i, j int) bool {
		return kvs[i][0] < kvs[j][0]
	})

	os.Remove(oname)

	for _, v := range kvs {
		writeFile(oname, v[0]+" "+v[1]+"\n")
	}

	// waiting all workers exit
	c.exit = true
	time.Sleep(time.Second * 10)

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
		//println("Init File ", i, files[i])

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
