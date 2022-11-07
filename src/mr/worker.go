package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	for {
		task := callAllocateTask()

		if task.Type == TaskTypeMap {
			fileName := task.FileName

			file, err := os.Open(fileName)
			if err != nil {
				log.Fatalf("cannot open %v", fileName)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", fileName)
			}
			file.Close()

			kvs := mapf(fileName, string(content))

			buckets := make([][]KeyValue, task.ReduceLength)

			for i := 0; i < len(kvs); i++ {
				pos := ihash(kvs[i].Key) % task.ReduceLength
				buckets[pos] = append(buckets[pos], kvs[i])
			}

			for i := 0; i < len(buckets); i++ {
				tempFile, err := os.CreateTemp(".", "mr-temp-*")
				if err != nil {
					log.Fatalf("cannot create %v", tempFile.Name())
				}

				encoder := json.NewEncoder(tempFile)
				encoder.Encode(buckets[i])
				tempFile.Close()

				fileName := fmt.Sprintf("mr-%v-%v", task.Number, i)
				err = os.Rename(tempFile.Name(), fileName)
				if err != nil {
					log.Fatalf("cannot rename %v to %v", tempFile.Name(), fileName)
				}
			}

			callFinishTask(task.Type, task.Number)
			continue
		}

		if task.Type == TaskTypeReduce {
			var kvs []KeyValue

			for i := 0; i < task.MapLength; i++ {
				var kvsInFile []KeyValue

				fileName := fmt.Sprintf("mr-%v-%v", i, task.Number)
				file, err := os.Open(fileName)
				if err != nil {
					log.Fatalf("cannot open %v", fileName)
				}
				decoder := json.NewDecoder(file)
				decoder.Decode(&kvsInFile)
				file.Close()

				kvs = append(kvs, kvsInFile...)
			}

			tempFile, err := os.CreateTemp(".", "mr-temp-*")
			if err != nil {
				log.Fatalf("cannot create %v", tempFile.Name())
			}

			reduceMap := make(map[string][]string)
			for i := 0; i < len(kvs); i++ {
				reduceMap[kvs[i].Key] = append(reduceMap[kvs[i].Key], kvs[i].Value)
			}

			for key, values := range reduceMap {
				result := reducef(key, values)
				tempFile.WriteString(fmt.Sprintf("%v %v\n", key, result))
			}
			tempFile.Close()

			fileName := fmt.Sprintf("mr-out-%v", task.Number)
			err = os.Rename(tempFile.Name(), fileName)
			if err != nil {
				log.Fatalf("cannot rename %v to %v", tempFile.Name(), fileName)
			}

			callFinishTask(task.Type, task.Number)
			continue
		}

		if task.Type == TaskTypeWait {
			time.Sleep(100 * time.Microsecond)
			continue
		}

		break
	}
}

func callAllocateTask() AllocateTaskReply {
	var args AllocateTaskArgs
	var reply AllocateTaskReply
	call("Coordinator.AllocateTask", &args, &reply)
	return reply
}

func callFinishTask(taskType string, taskNumber int) {
	var args FinishTaskArgs
	var reply FinishTaskReply
	args.Type = taskType
	args.Number = taskNumber
	call("Coordinator.FinishTask", &args, &reply)
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
