package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string `json:"key"`
	Value string `json:"Value"`
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// 获取mr-X-Y中的Y
func getReduceNum(key string, nReduce int) int {
	return ihash(key) % nReduce
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	for {
		reply := askTask()

		switch reply.WorkerState {
		case Map:
			MapData(&reply, mapf)
		case Reduce:
			ReduceData(&reply, reducef)
		case Wait:
			// 等待5秒钟，重新分配任务
			time.Sleep(time.Duration(5) * time.Second)
		default:
			return
		}
	}
}

// map
func MapData(task *Task, mapf func(string, string) []KeyValue) {

	// 先读取文件内容
	content, err := os.ReadFile(task.FileName)

	if err != nil {
		log.Fatalf("cannot read %v", task.FileName)
	}

	// 中间结果
	intermediates := mapf(task.FileName, string(content))

	// 存放
	buffer := make([][]KeyValue, task.NReduce)
	// 将相同的数据换分到同一文件中
	for _, kv := range intermediates {
		slot := getReduceNum(kv.Key, task.NReduce)
		buffer[slot] = append(buffer[slot], kv)
	}

	// 当前map处理的文件地址集合
	var mapOutput []string

	for i := 0; i < task.NReduce; i++ {
		fileName := fmt.Sprintf("mr-%d-%d", task.TaskNum, i)

		ofile, _ := os.Create(fileName)
		encoder := json.NewEncoder(ofile)
		// 写入磁盘
		for _, kv := range buffer[i] {
			err := encoder.Encode(&kv)
			if err != nil {
				log.Fatal("jsonEncode err", err)
			}
		}
		mapOutput = append(mapOutput, fileName)
		ofile.Close()
	}

	task.Intermediates = mapOutput

	TaskCompleted(task)
}

func TaskCompleted(task *Task) {
	reply := ExampleReply{}
	call("Coordinator.TaskCompleted", &task, &reply)
}

// reduce
func ReduceData(task *Task, reducef func(string, []string) string) {
	// 获取同一个reduce task number的文件数据

	//fmt.Println(task.Intermediates)

	intermediates := ReadFromLocalFile(task)

	// 排序
	sort.Sort(ByKey(intermediates))

	dir, _ := os.Getwd()
	tempFile, err := ioutil.TempFile(dir, "mr-tmp-*")
	if err != nil {
		log.Fatalf("cannot open create temp file")
	}

	i := 0
	for i < len(intermediates) {
		j := i + 1
		for j < len(intermediates) && intermediates[j].Key == intermediates[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, intermediates[k].Value)
		}
		output := reducef(intermediates[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(tempFile, "%v %v\n", intermediates[i].Key, output)

		i = j
	}

	tempFile.Close()
	oname := fmt.Sprintf("mr-out-%d", task.TaskNum)
	os.Rename(tempFile.Name(), oname)

	// 将输出路径返回回去
	task.Output = oname
	TaskCompleted(task)
}

// 取出当前reduce的所有keyValue
func ReadFromLocalFile(task *Task) []KeyValue {

	var intermediates []KeyValue

	for _, filepath := range task.Intermediates {
		file, err := os.Open(filepath)
		if err != nil {
			log.Fatalf("cannot open %v", filepath)
		}

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediates = append(intermediates, kv)
		}
		file.Close()
	}

	return intermediates
}

// 请求任务(matser根据流程阶段分配任务)
func askTask() Task {
	args := ExampleArgs{}
	reply := Task{}
	call("Coordinator.GenerateTask", &args, &reply)
	return reply
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
