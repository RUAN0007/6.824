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

type Command int

const (
	MapOp = iota
	ReduceOp
	NoOp
)

func (d Command) String() string {
	return [...]string{"MapOp", "ReduceOp", "NoOp"}[d]
}

type TaskRequest struct {
	WorkPid int
}

// If Cmd == Map: args = [mapTaskID, mapParameter], outputFiles = a list of intermediate files
// If Cmd == Reduce: args = [reduceTaskID, a list of intermediate files...], outputFiles = [output-file]

type TaskReply struct {
	Cmd         Command
	Args        []string
	OutputFiles []string // Reduce task only has a single output file
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
