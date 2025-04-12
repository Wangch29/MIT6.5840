package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type SortedKey []KeyValue

func (s SortedKey) Len() int           { return len(s) }
func (s SortedKey) Less(i, j int) bool { return s[i].Key < s[j].Key }
func (s SortedKey) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

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
				callDone(&task)
			}

		case ReduceTask:
			{
				DoReduceTask(reducef, &task)
				callDone(&task)
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
		fmt.Printf("Receive a task, task type:%v, taskId:%v\n", reply.TaskType, reply.TaskId)
	} else {
		fmt.Println("Fail to get a task!")
	}

	return reply
}

// DoMapTask does the core work of using map function.
func DoMapTask(mapf func(string, string) []KeyValue, task *Task) {
	var filename string = task.FileNames[0]

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

// DoReduceTask does the core work of using reduce function.
func DoReduceTask(reducef func(string, []string) string, task *Task) {
	intermediate := shuffle(task.FileNames)

	dir, _ := os.Getwd()
	tmpFile, err := os.CreateTemp(dir, "mr-tmp-reduce-*")
	if err != nil {
		log.Fatal("Failed to create temp file", err)
	}

	// Collect values of the same key.
	i := 0
	for i < len(intermediate) {
		var values []string
		j := i
		for j < len(intermediate) && intermediate[i].Key == intermediate[j].Key {
			values = append(values, intermediate[j].Value)
			j++
		}
		// apply reduce function
		res := reducef(intermediate[i].Key, values)
		fmt.Fprintf(tmpFile, "%v %v\n", intermediate[i].Key, res)
		i = j
	}

	// Close and rename tmpFile.
	tmpFile.Close()
	fn := fmt.Sprintf("mr-out-%d", task.TaskId)
	os.Rename(tmpFile.Name(), fn)
}

// shuffle reads all
func shuffle(files []string) []KeyValue {
	var kvpairs []KeyValue

	// Read files and add KV to kvpairs.
	for _, fn := range files {
		file, err := os.Open(fn)
		if err != nil {
			continue
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kvpairs = append(kvpairs, kv)
		}
		file.Close()
	}

	sort.Sort(SortedKey(kvpairs))
	return kvpairs
}

// Call rpc to mark task as completed.
func callDone(task *Task) Task {
	reply := Task{}

	ok := call("Coordinator.MarkFinished", task, &reply)

	if ok {
		// fmt.Println(reply)
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
