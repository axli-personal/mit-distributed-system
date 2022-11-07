package mr

import (
	"os"
	"strconv"
)

type AllocateTaskArgs struct {
}

type AllocateTaskReply struct {
	Type         string
	Number       int
	FileName     string
	MapLength    int
	ReduceLength int
}

type FinishTaskArgs struct {
	Type   string
	Number int
}

type FinishTaskReply struct {
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
