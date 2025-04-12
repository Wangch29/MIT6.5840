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

type TaskArgs struct{}

type TaskType int

// Task types
const (
	MapTask TaskType = iota
	ReduceTask
	WaitTask
	ExitTask
)

type Phase int

const (
	MapPhase Phase = iota
	ReducePhase
	AllDone
)

type State int

// Task states
const (
	Working State = iota
	Waiting
	Done
)

// Task structure.
type Task struct {
	TaskType  TaskType // Task type, map or reduce.
	TaskId    int      // Task id
	ReduceNum int      // Number of reducers.
	FileNames []string // Input filename.
}

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
