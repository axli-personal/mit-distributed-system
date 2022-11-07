package mr

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const (
	TaskTypeWait   = "Wait"
	TaskTypeMap    = "Map"
	TaskTypeReduce = "Reduce"
	TaskTypeDone   = "Done"
)

const (
	FlagRunnable = iota
	FlagRunning
	FlagDone
)

type Coordinator struct {
	mutex       sync.Mutex
	maxWaitTime time.Duration
	fileNames   []string
	mapCount    int
	mapFlag     []int8
	mapClock    []time.Time
	reduceCount int
	reduceFlag  []int8
	reduceClock []time.Time
}

func (c *Coordinator) AllocateTask(args *AllocateTaskArgs, reply *AllocateTaskReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.mapCount > 0 {
		for i := 0; i < len(c.mapFlag); i++ {
			if c.mapFlag[i] == FlagRunnable {
				c.mapFlag[i] = FlagRunning
				c.mapClock[i] = time.Now()
				reply.Type = TaskTypeMap
				reply.Number = i
				reply.ReduceLength = len(c.reduceFlag)
				reply.FileName = c.fileNames[i]
				return nil
			}
		}
		for i := 0; i < len(c.mapFlag); i++ {
			if c.mapFlag[i] == FlagRunning && c.mapClock[i].Add(c.maxWaitTime).Before(time.Now()) {
				c.mapClock[i] = time.Now()
				reply.Type = TaskTypeMap
				reply.Number = i
				reply.ReduceLength = len(c.reduceFlag)
				reply.FileName = c.fileNames[i]
				return nil
			}
		}
		reply.Type = TaskTypeWait
		return nil
	}

	if c.reduceCount > 0 {
		for i := 0; i < len(c.reduceFlag); i++ {
			if c.reduceFlag[i] == FlagRunnable {
				c.reduceFlag[i] = FlagRunning
				c.reduceClock[i] = time.Now()
				reply.Type = TaskTypeReduce
				reply.Number = i
				reply.MapLength = len(c.mapFlag)
				return nil
			}
		}
		for i := 0; i < len(c.reduceFlag); i++ {
			if c.reduceFlag[i] == FlagRunning && c.reduceClock[i].Add(c.maxWaitTime).Before(time.Now()) {
				c.reduceClock[i] = time.Now()
				reply.Type = TaskTypeReduce
				reply.Number = i
				reply.MapLength = len(c.mapFlag)
				return nil
			}
		}
		reply.Type = TaskTypeWait
		return nil
	}

	reply.Type = TaskTypeDone
	return nil
}

func (c *Coordinator) FinishTask(args *FinishTaskArgs, reply *FinishTaskReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if args.Type == TaskTypeMap {
		if c.mapFlag[args.Number] == FlagRunning {
			c.mapFlag[args.Number] = FlagDone
			c.mapCount -= 1
			return nil
		}
		return errors.New("task is not running")
	}
	if args.Type == TaskTypeReduce {
		if c.reduceFlag[args.Number] == FlagRunning {
			c.reduceFlag[args.Number] = FlagDone
			c.reduceCount -= 1
			return nil
		}
		return errors.New("task is not running")
	}
	return errors.New("invalid task type")
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return c.mapCount == 0 && c.reduceCount == 0
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		maxWaitTime: 5 * time.Second,
		fileNames:   files,
		mapCount:    len(files),
		mapFlag:     make([]int8, len(files)),
		mapClock:    make([]time.Time, len(files)),
		reduceCount: nReduce,
		reduceFlag:  make([]int8, nReduce),
		reduceClock: make([]time.Time, nReduce),
	}

	c.server()

	return &c
}
