package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

var mu sync.Mutex

type Coordinator struct {
	ReducerNum     int
	GlobalTaskId   int
	DistPhase      Phase
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
	state   State
	TaskPtr *Task // Pointing to task
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
		TaskChanReduce: make(chan *Task, len(files)),
		TaskMetaHolder: TaskMetaHolder{
			MetaMap: make(map[int]*TaskMetaInfo, len(files)+nReduce),
		},
	}

	c.makeMapTasks(files)

	c.server()
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
			FileName:  file,
		}
		taskMetaInfo := TaskMetaInfo{
			state:   Waiting,
			TaskPtr: &task,
		}
		c.TaskMetaHolder.acceptMeta(&taskMetaInfo)

		fmt.Println("make a map task :", &task)
		c.TaskChanMap <- &task
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
		fmt.Println("meta contains task with id = ", taskId)
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
				c.TaskMetaHolder.judgeState(reply.TaskId)
			} else {
				// Distribute all map tasks but not finished, set type to WaitTask.
				reply.TaskType = WaitTask
				if c.TaskMetaHolder.checkTaskDone() {
					c.toNextPhase()
				}
			}
		}

	default:
		{
			reply.TaskType = ExitTask
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

	// TODO: refine logic?
	if (mapDoneNum > 0 && mapUnDoneNum == 0) && (reduceDoneNum == 0 && reduceUnDoneNum == 0) {
		return true
	}
	if reduceDoneNum > 0 && reduceUnDoneNum == 0 {
		return true
	}

	return false
}

func (c *Coordinator) toNextPhase() {
	if c.DistPhase == MapPhase {
		// c.makeReduceTasks()
		c.DistPhase = AllDone
	} else if c.DistPhase == ReducePhase {
		c.DistPhase = AllDone
	}
}

func (t *TaskMetaHolder) judgeState(taskId int) bool {
	info, ok := t.MetaMap[taskId]
	if !ok || info.state != Waiting {
		return false
	}
	info.state = Working
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

	default:
		panic("The task type undefined ! ! !")
	}

	return nil
}
