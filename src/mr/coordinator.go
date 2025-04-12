package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strings"
	"sync"
	"time"
)

// Global mutex to protect coordinator
var mu sync.Mutex

type Coordinator struct {
	ReducerNum     int
	GlobalTaskId   int
	DistPhase      Phase // Distribute phase, Map -> Reduce -> Alldone
	TaskChanMap    chan *Task
	TaskChanReduce chan *Task
	TaskMetaHolder TaskMetaHolder
	Files          []string
}

// TaskMetaHolder stores all tasks' metadata
type TaskMetaHolder struct {
	MetaMap map[int]*TaskMetaInfo
}

// TaskMetaInfo store task's metadata
type TaskMetaInfo struct {
	state     State
	StartTime time.Time
	TaskPtr   *Task // Pointing to task
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// server starts a thread that listens for RPCs from worker.go
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
	mu.Lock()
	defer mu.Unlock()

	if c.DistPhase == AllDone {
		fmt.Printf("All tasks are finished,the coordinator will be exit! !")
		return true
	}

	return false
}

// MakeCoordinator creates a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		Files:          files,
		ReducerNum:     nReduce,
		GlobalTaskId:   0,
		DistPhase:      MapPhase,
		TaskChanMap:    make(chan *Task, len(files)),
		TaskChanReduce: make(chan *Task, nReduce),
		TaskMetaHolder: TaskMetaHolder{
			MetaMap: make(map[int]*TaskMetaInfo, len(files)+nReduce),
		},
	}

	c.makeMapTasks(files)

	c.server()

	go c.CrashHandler()

	return &c
}

// makeMapTasks read input files and initialize tasks.
func (c *Coordinator) makeMapTasks(files []string) {
	for _, file := range files {
		id := c.generateTaskId()
		task := Task{
			TaskType:  MapTask,
			TaskId:    id,
			ReduceNum: c.ReducerNum,
			FileNames: []string{file},
		}
		taskMetaInfo := TaskMetaInfo{
			state:   Waiting,
			TaskPtr: &task,
		}
		c.TaskMetaHolder.acceptMeta(&taskMetaInfo)

		// fmt.Println("make a map task :", &task)
		c.TaskChanMap <- &task
	}
}

func (c *Coordinator) makeReduceTasks() {
	for i := 0; i < c.ReducerNum; i++ {
		id := c.generateTaskId()
		task := Task{
			TaskType:  ReduceTask,
			TaskId:    id,
			ReduceNum: c.ReducerNum,
			FileNames: selectReduceName(i),
		}
		taskMetaInfo := TaskMetaInfo{
			state:   Waiting,
			TaskPtr: &task,
		}
		c.TaskMetaHolder.acceptMeta(&taskMetaInfo)

		// fmt.Println("make a reduce task :", task.TaskId)
		c.TaskChanReduce <- &task
	}
}

func (c *Coordinator) generateTaskId() int {
	re := c.GlobalTaskId
	c.GlobalTaskId += 1
	return re
}

func (t *TaskMetaHolder) acceptMeta(taskMetaInfo *TaskMetaInfo) bool {
	taskId := taskMetaInfo.TaskPtr.TaskId
	_, ok := t.MetaMap[taskId]
	if ok {
		// fmt.Println("meta contains task with id = ", taskId)
		return false
	} else {
		t.MetaMap[taskId] = taskMetaInfo
		return true
	}
}

// PollTask send tasks to callers.
// reply is the returned task.
func (c *Coordinator) PollTask(taskArgs *TaskArgs, reply *Task) error {
	mu.Lock()
	defer mu.Unlock()

	switch c.DistPhase {
	case MapPhase:
		{
			if len(c.TaskChanMap) > 0 {
				*reply = *<-c.TaskChanMap
				c.TaskMetaHolder.beginTask(reply.TaskId)
			} else {
				// Distribute all map tasks but not finished, set type to WaitTask.
				reply.TaskType = WaitTask
				if c.TaskMetaHolder.checkTaskDone() {
					c.toNextPhase()
				}
			}
		}

	case ReducePhase:
		{
			if len(c.TaskChanReduce) > 0 {
				*reply = *<-c.TaskChanReduce
				c.TaskMetaHolder.beginTask(reply.TaskId)
			} else {
				// Distribute all reduce tasks but not finished, set type to WaitTask.
				reply.TaskType = WaitTask
				if c.TaskMetaHolder.checkTaskDone() {
					c.toNextPhase()
				}
			}
		}

	case AllDone:
		{
			reply.TaskType = ExitTask
		}

	default:
		{
			panic("Phase undefined!")
		}
	}

	return nil
}

func (t *TaskMetaHolder) checkTaskDone() bool {
	var (
		mapDoneNum      = 0
		mapUnDoneNum    = 0
		reduceDoneNum   = 0
		reduceUnDoneNum = 0
	)

	for _, v := range t.MetaMap {
		if v.TaskPtr.TaskType == MapTask {
			if v.state == Done {
				mapDoneNum++
			} else {
				mapUnDoneNum++
			}
		} else if v.TaskPtr.TaskType == ReduceTask {
			if v.state == Done {
				reduceDoneNum++
			} else {
				reduceUnDoneNum++
			}
		}
	}

	//fmt.Printf("map tasks  are finished %d/%d, reduce task are finished %d/%d \n",
	//	mapDoneNum, mapDoneNum+mapUnDoneNum, reduceDoneNum, reduceDoneNum+reduceUnDoneNum)

	if (mapDoneNum > 0 && mapUnDoneNum == 0) && (reduceDoneNum == 0 && reduceUnDoneNum == 0) {
		// MapPhase done.
		return true
	}
	if (reduceDoneNum > 0 && reduceUnDoneNum == 0) && (mapDoneNum > 0 && mapUnDoneNum == 0) {
		// ReducePhase done.
		return true
	}

	return false
}

func (c *Coordinator) toNextPhase() {
	if c.DistPhase == MapPhase {
		c.makeReduceTasks()
		c.DistPhase = ReducePhase
	} else if c.DistPhase == ReducePhase {
		c.DistPhase = AllDone
	}
}

// beginTask turns task state from Waiting to Working.
// return false if the task state was not Waiting.
func (t *TaskMetaHolder) beginTask(taskId int) bool {
	info, ok := t.MetaMap[taskId]
	if !ok || info.state != Waiting {
		return false
	}
	info.state = Working
	info.StartTime = time.Now()
	return true
}

func (c *Coordinator) MarkFinished(args *Task, reply *Task) error {
	mu.Lock()
	defer mu.Unlock()

	switch args.TaskType {

	case MapTask:
		meta, ok := c.TaskMetaHolder.MetaMap[args.TaskId]
		if ok && meta.state == Working {
			meta.state = Done
			fmt.Printf("Map task Id[%d] is finished.\n", args.TaskId)
		} else {
			fmt.Printf("Map task Id[%d] is finished,already ! ! !\n", args.TaskId)
		}

	case ReduceTask:
		meta, ok := c.TaskMetaHolder.MetaMap[args.TaskId]
		if ok && meta.state == Working {
			meta.state = Done
			fmt.Printf("Reduce task Id[%d] is finished.\n", args.TaskId)
		} else {
			fmt.Printf("Reduce task Id[%d] is finished,already ! ! !\n", args.TaskId)
		}

	default:
		panic("The task type undefined ! ! !")
	}

	return nil
}

func selectReduceName(reduceIdx int) []string {
	dirpath, _ := os.Getwd()
	suffix := fmt.Sprintf("-%d", reduceIdx)

	files, err := findFilesWithPrefixSuffix(dirpath+"/", "mr-tmp-", suffix)
	if err != nil {
		fmt.Printf("cannot read directory: %v", dirpath)
		return []string{}
	}
	return files
}

// findFilesWithPrefixSuffix finds all files with certain prefix and suffix in a directory.
func findFilesWithPrefixSuffix(dir, prefix, suffix string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	var matched []string
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if strings.HasPrefix(name, prefix) && strings.HasSuffix(name, suffix) {
			matched = append(matched, dir+name)
		}
	}
	return matched, nil
}

// CrashHandler keep detecting if there is task working longer than 10 seconds.
// If so, add the task back to waiting channel for re-schedule.
func (c *Coordinator) CrashHandler() {
	for {
		time.Sleep(time.Second * 2)

		mu.Lock()

		if c.DistPhase == AllDone {
			mu.Unlock()
			return
		}

		for _, meta := range c.TaskMetaHolder.MetaMap {
			if meta.state == Working && time.Since(meta.StartTime) > 10*time.Second {
				if meta.TaskPtr.TaskType == MapTask {
					c.TaskChanMap <- meta.TaskPtr
				} else if meta.TaskPtr.TaskType == ReduceTask {
					c.TaskChanReduce <- meta.TaskPtr
				}
				meta.state = Waiting
			}
		}

		mu.Unlock()
	}
}
