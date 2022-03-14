package mr

import (
	"fmt"
	"log"
	"math"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type TaskQueue struct {
	tasks []Task
}

type TaskQueuer interface {
	New() TaskQueue
	Enqueue(t Task)
	Dequeue() *Task
	IsEmpty() bool
	Size() int
}

// 初始化队列
func (q *TaskQueue) New() *TaskQueue {
	q.tasks = []Task{}
	return q
}

// 入队列
func (q *TaskQueue) Enqueue(t Task) {
	q.tasks = append(q.tasks, t)
}

// 出队列
func (q *TaskQueue) Dequeue() *Task {
	t := &q.tasks[0]
	q.tasks = q.tasks[1:len(q.tasks)]
	return t
}

// 大小
func (q *TaskQueue) Size() int {
	return len(q.tasks)
}

// 是否为空
func (q *TaskQueue) IsEmpty() bool {
	return q.Size() == 0
}


type State int

// 定义整个工作流程的阶段，只有上一个阶段完成才能进入下一个阶段
const (
	Map    State = iota // map阶段
	Reduce              // reduce阶段
	Wait
	Exit
)

type Coordinator struct {
	// Your definitions here.
	InputFiles  []string // 输入文件名称列表
	OutputFiles []string // 输出文件名称列表
	Queue       TaskQueue
	NReduce     int
	MasterPhase State // 当前处于哪个阶段
	TaskMap     map[int]*Task
	OutputPath  [][]string // map阶段后中间结果存放文件名位置的集合(由mapworker发送过来并保存，由reducework询问并返回）
	Lck         sync.Mutex // 锁
}

func (c *Coordinator) TaskCompleted(task *Task, reply *ExampleReply) error {
	c.Lck.Lock()
	defer c.Lck.Unlock()

	// 判断是否重复做任务
	if c.MasterPhase != task.WorkerState || c.TaskMap[task.TaskNum].TaskPhase == Completed {
		return nil
	}

	c.TaskMap[task.TaskNum].TaskPhase = Completed

	c.processTaskResult(task)
	return nil
}

func (c *Coordinator) processTaskResult(task *Task) {

	// fmt.Println("processTaskResult")
	switch task.WorkerState {
	case Map:
		for taskNum, intermediate := range task.Intermediates {
			c.OutputPath[taskNum] = append(c.OutputPath[taskNum], intermediate)
		}

		if c.isAllCompleted() {
			fmt.Println("switch reduce")
			c.MasterPhase = Reduce
			c.initReduceTask()
		}
	case Reduce:
		if c.isAllCompleted() {
			fmt.Println("switch Exit")
			c.MasterPhase = Exit
		}
	}

}

// 判断是否所有的任务已经完成，用于转换work流程
func (c *Coordinator) isAllCompleted() bool {
	for _, task := range c.TaskMap {
		if task.TaskPhase != Completed {
			return false
		}
	}
	return true
}

// 分配任务
func (c *Coordinator) GenerateTask(args *ExampleArgs, reply *Task) error {
	c.Lck.Lock()
	defer c.Lck.Unlock()

	if !c.Queue.IsEmpty() {
		task := *(c.Queue.Dequeue())
		c.TaskMap[task.TaskNum].StartTime = time.Now()
		c.TaskMap[task.TaskNum].TaskPhase = InProgress
		*reply = task
	} else if c.MasterPhase == Exit {
		*reply = Task{WorkerState: Exit}
	} else {
		*reply = Task{WorkerState: Wait}
	}

	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	c.Lck.Lock()
	defer c.Lck.Unlock()

	// Your code here.
	ret := c.MasterPhase == Exit
	return ret
}

// 初始化reduce任务
func (c *Coordinator) initReduceTask() {

	c.Queue = TaskQueue{}
	c.Queue.New()

	// 初始化任务
	for i := 0; i < c.NReduce; i++ {
		// 建立索引

		c.TaskMap[i] = &Task{
			TaskNum:     i,
			TaskPhase:   Idle,
			WorkerState: Reduce,
			// StartTime:   time.Now(),
			Intermediates: c.OutputPath[i],
			NReduce:       c.NReduce,
			StartTime:     time.Now(),
		}

		// 任务队列
		c.Queue.Enqueue(*(c.TaskMap[i]))
	}

	go c.checkTaskCrash()

}

func (c *Coordinator) initMapTask(files []string, nReduce int) {

	// Your code here.
	// 初始化任务队列
	c.Queue = TaskQueue{}
	c.Queue.New()

	// 初始化任务
	for i := 0; i < len(files); i++ {
		// 建立索引
		c.TaskMap[i] = &Task{
			TaskNum:     i,
			TaskPhase:   Idle,
			FileName:    files[i],
			WorkerState: Map,
			StartTime:   time.Now(),
			NReduce:     nReduce,
		}

		// 任务队列
		c.Queue.Enqueue(*(c.TaskMap[i]))
	}

	go c.checkTaskCrash()
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		InputFiles:  files,
		OutputFiles: make([]string, 0),
		NReduce:     nReduce,
		MasterPhase: Map,                                                                         // 当前处理阶段
		TaskMap:     make(map[int]*Task, (int)(math.Max(float64(len(files)), float64(nReduce)))), // 当前任务
		OutputPath:  make([][]string, nReduce),
	}

	c.initMapTask(files, nReduce)

	// go c.checkTaskCrash()
	c.server()
	return &c
}

func (c *Coordinator) checkTaskCrash() {

	for {
		// 每过一秒检测一次
		time.Sleep(time.Duration(8) * time.Second)

		if c.MasterPhase == Exit{
			break
		}

		c.Lck.Lock()
		for index, task := range c.TaskMap {
			if time.Since(task.StartTime) > time.Second * time.Duration(10) && task.TaskPhase == InProgress{
				c.TaskMap[index].StartTime = time.Now()
				c.TaskMap[index].TaskPhase = Idle
				// 任务队列
				c.Queue.Enqueue(*(c.TaskMap[index]))
			}
		}
		c.Lck.Unlock()
	}

}
