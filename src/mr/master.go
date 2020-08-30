package mr

import (
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"

	"golang.org/x/sync/semaphore"
)

type Master struct {
	doneMutex sync.Mutex
	done      bool
	// Your definitions here.
	workerIDs map[int]bool

	mapJobs    []mapJob
	reduceJobs []reduceJob

	nReduce int
}

type jobSatus int

const (
	idle       jobSatus = iota
	inProgress jobSatus = iota
	complete   jobSatus = iota
)

type mapJob struct {
	sem  *semaphore.Weighted
	js   jobSatus
	file string
}

type reduceJob struct {
	sem            *semaphore.Weighted
	immediateFiles []string
	js             jobSatus
	outputFile     string
}

// Your code here -- RPC handlers for the worker to call.

func (m *Master) MapJobComplete(args *MapJobCompleteArgs, reply *MapJobCompleteReply) error {

	if m.mapJobs[args.TaskNumber].sem.TryAcquire(1) {
		defer m.mapJobs[args.TaskNumber].sem.Release(1)

		m.mapJobs[args.TaskNumber].js = complete
		for _, immediateFile := range args.ImmediateFiles {
			if m.reduceJobs[immediateFile.ReduceTaskBucket].sem.TryAcquire(1) {
				defer m.reduceJobs[immediateFile.ReduceTaskBucket].sem.Release(1)
				m.reduceJobs[immediateFile.ReduceTaskBucket].immediateFiles = append(m.reduceJobs[immediateFile.ReduceTaskBucket].immediateFiles, immediateFile.File)
			}
		}

	}

	return nil
}

func (m *Master) ReduceJobComplete(args *ReduceJobCompleteArgs, reply *ReduceJobCompleteReply) error {
	if m.reduceJobs[args.TaskNumber].sem.TryAcquire(1) {
		defer m.reduceJobs[args.TaskNumber].sem.Release(1)

		m.reduceJobs[args.TaskNumber].js = complete
		m.reduceJobs[args.TaskNumber].outputFile = args.FileName
	}

	return nil
}

func (m *Master) RequestJob(args *JobRequestArgs, reply *JobRequestReply) error {
	numOfCompleteMapJobs := 0
	for i, job := range m.mapJobs {
		if job.sem.TryAcquire(1) {
			defer job.sem.Release(1)
			if job.js == idle {

				reply.TypeOfJob = Map
				reply.Files = []string{job.file}
				reply.TaskNumber = i
				reply.NReduce = m.nReduce

				m.mapJobs[i].js = inProgress

				go func(jt JobType, jobIndex int) {
					time.Sleep(time.Second * 10)
					res := m.checkJobCompletion(jt, jobIndex)
					for !res {
						res = m.checkJobCompletion(jt, jobIndex)
					}
				}(Map, i)
				return nil
			} else if job.js == complete {
				numOfCompleteMapJobs++
			}
		}
	}

	numOfCompleteReduceJobs := 0

	if numOfCompleteMapJobs == len(m.mapJobs) { // all of map jobs are complete, start reduce
		for i, job := range m.reduceJobs {

			if job.sem.TryAcquire(1) {
				defer job.sem.Release(1)
				if job.js == idle {

					reply.TypeOfJob = Reduce
					reply.Files = job.immediateFiles
					reply.TaskNumber = i
					reply.NReduce = m.nReduce

					m.reduceJobs[i].js = inProgress

					go func(jt JobType, jobIndex int) {
						time.Sleep(time.Second * 10)
						res := m.checkJobCompletion(jt, jobIndex)
						for !res {
							res = m.checkJobCompletion(jt, jobIndex)
						}
					}(Reduce, i)
					return nil
				} else if job.js == complete {
					numOfCompleteReduceJobs++
				}
			}
		}
	}

	reply.TypeOfJob = NoJob

	if numOfCompleteReduceJobs == m.nReduce {
		m.doneMutex.Lock()
		m.done = true
		defer m.doneMutex.Unlock()
	}
	return nil
}

func (m *Master) checkJobCompletion(jt JobType, jobIndex int) bool {
	if jt == Map {
		if m.mapJobs[jobIndex].sem.TryAcquire(1) {
			defer m.mapJobs[jobIndex].sem.Release(1)
			if m.mapJobs[jobIndex].js == inProgress {
				m.mapJobs[jobIndex].js = idle
			}
			return true
		}
	} else if jt == Reduce {
		if m.reduceJobs[jobIndex].sem.TryAcquire(1) {
			defer m.reduceJobs[jobIndex].sem.Release(1)
			if m.reduceJobs[jobIndex].js == inProgress {
				m.reduceJobs[jobIndex].js = idle
			}

			return true
		}
	} else {

	}

	return false
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		// log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	m.doneMutex.Lock()
	defer m.doneMutex.Unlock()

	ret := m.done

	// Your code here.

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	m.nReduce = nReduce
	m.done = false
	// Your code here.
	for _, file := range files {
		job := mapJob{semaphore.NewWeighted(1), idle, file}

		m.mapJobs = append(m.mapJobs, job)
	}

	m.reduceJobs = make([]reduceJob, nReduce, nReduce)
	for i := 0; i < nReduce; i++ {
		m.reduceJobs[i] = reduceJob{semaphore.NewWeighted(1), []string{}, idle, "mr-out-" + strconv.Itoa(i)}
	}
	m.server()
	return &m
}
