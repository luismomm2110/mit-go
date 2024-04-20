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

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type MapArgs struct{}

type MapReply struct {
	Task               *MapTask
	NumberReducerTasks int
}

type FinishMapTaskArgs struct {
	Task  *MapTask
	Files []string
}

type FinishMapTaskReply struct {
	IsReduceReady bool
}

type ReduceTaskArgs struct{}

type ReduceTaskReply struct {
	Task *ReduceTask
}

type FinishReduceTaskArgs struct {
	Task int
}

type FinishReduceTaskReply struct {
	IsFinished bool
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
