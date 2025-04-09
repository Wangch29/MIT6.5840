package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"strconv"
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
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	keepFlag := true

	for keepFlag {
		task := GetTask()
		switch task.TaskType {
		case MapTask:
			{
				DoMapTask(mapf, &task)
				callDone(task.TaskId)
			}

		case WaitTask:
			{
				fmt.Println("All tasks are in progress, please waiting...")
				time.Sleep(time.Second)
			}

		case ExitTask:
			{
				fmt.Println("Task about :[", task.TaskId, "] is terminated...")
				keepFlag = false
			}
		}
	}
}

// Get a task from Coordinator by rpc.
func GetTask() Task {
	args := TaskArgs{} // Empty args.
	reply := Task{}
	ok := call("Coordinator.PollTask", &args, &reply)

	if ok {
		fmt.Println(reply)
	} else {
		fmt.Println("call fail!")
	}

	return reply
}

// DoMapTask does the core work of using map function to handle the task.
func DoMapTask(mapf func(string, string) []KeyValue, task *Task) {
	var filename string = task.FileName

	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open file %v", filename)
	}

	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read file %v", filename)
	}
	file.Close()

	var intermediate []KeyValue = mapf(filename, string(content))

	rn := task.ReduceNum
	HashedKV := make([][]KeyValue, rn)

	// Distribute KV pairs to different reduce.
	for _, kv := range intermediate {
		index := ihash(kv.Key) % rn
		HashedKV[index] = append(HashedKV[index], kv)
	}

	// Write KV pairs of each reduce to a file.
	for i := 0; i < rn; i++ {
		oname := "mr-tmp-" + strconv.Itoa(task.TaskId) + "-" + strconv.Itoa(i)
		ofile, err := os.Create(oname)
		if err != nil {
			log.Fatalf("cannot create file %v", oname)
		}

		enc := json.NewEncoder(ofile)
		for _, kv := range HashedKV[i] {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatalf("cannot encode kv pair %v", kv)
			}
		}
		ofile.Close()
	}
}

// Call rpc to mark task as completed.
func callDone(taskId int) Task {
	args := Task{}
	reply := Task{}

	args.TaskId = taskId
	ok := call("Coordinator.MarkFinished", &args, &reply)

	if ok {
		fmt.Println(reply)
	} else {
		fmt.Println("call fail!")
	}

	return reply
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
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
