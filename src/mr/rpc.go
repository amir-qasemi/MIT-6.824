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
type MapJobCompleteArgs struct {
	TaskNumber     int
	ImmediateFiles []ImmediateFile
}
type MapJobCompleteReply struct {
}

type ReduceJobCompleteArgs struct {
	TaskNumber int
	FileName   string
}

type ReduceJobCompleteReply struct {
}

type ImmediateFile struct {
	File             string
	ReduceTaskBucket int
}

type JobType int

const (
	NoJob          = iota
	Map    JobType = iota
	Reduce JobType = iota
)

type JobRequestArgs struct {
	WorkerID int
}

type JobRequestReply struct {
	TypeOfJob JobType

	TaskNumber int
	NReduce    int
	Files      []string
}

// RegisterWorkerRequest is
type RegisterWorkerRequest struct {
}

// RegisterWorkerResponse is
type RegisterWorkerResponse struct {
	WorkerID int
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
