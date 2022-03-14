package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"time"
)
import "strconv"

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


type WorkTaskState int

// 定义map或reduce的任务阶段常量
const (
	Idle       WorkTaskState = iota // 空闲状态
	InProgress                      // 处理中
	Completed                       // 完成
)

type Task struct {
	TaskNum       int           // 任务编号
	TaskPhase     WorkTaskState // 当前work状态
	WorkerState   State         // 区分当前任务是map还是reduce
	FileName      string        // 当前处理哪个文件
	StartTime     time.Time     // map开始任务的时间
	Intermediates []string      // map完存放文件名位置的集合
	NReduce       int
	Output        string // reduce结束后输出的地址
	// IsTimeOut     bool   // 是否超时
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
