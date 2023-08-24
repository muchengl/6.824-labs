package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

type TaskGetArgs struct {
	Idx int
}

type NReduce struct {
	N int
}

type TaskReply struct {
	// idx=-1 reduce task
	// idx>=0 map task
	MapTaskIdx  int
	MapFilename string

	ReduceTaskIdx  int
	ReduceFileName string
}

type TaskFinishReply struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
