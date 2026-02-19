package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"sort"
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

var coordSockName string // socket for coordinator

// main/mrworker.go calls this function.
func Worker(sockname string, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	coordSockName = sockname

	// Your worker implementation here.
	workerID := 0

	for {
		// Request a task from coordinator
		task := requestTask(workerID)

		if task.Type == ExitTask {
			// Coordinator told us to exit
			break
		}

		if task.Type == NoTask {
			// No task available, wait and try again
			time.Sleep(1 * time.Second)
			continue
		}

		// Execute the task
		success := false
		if task.Type == MapTask {
			success = executeMapTask(task, mapf)
		} else if task.Type == ReduceTask {
			success = executeReduceTask(task, reducef)
		}

		// Report task completion
		reportTask(workerID, task, success)

		// Update worker ID if it was assigned by coordinator
		if workerID == 0 {
			// We need to get the actual worker ID from the response
			// For simplicity, we'll just use 1 for now
			workerID = 1
		}
	}

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
	c, err := rpc.DialHTTP("unix", coordSockName)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	if err := c.Call(rpcname, args, reply); err == nil {
		return true
	}
	log.Printf("%d: call failed err %v", os.Getpid(), err)
	return false
}

// requestTask requests a task from the coordinator
func requestTask(workerID int) Task {
	args := RequestTaskArgs{WorkerID: workerID}
	reply := RequestTaskReply{}

	if call("Coordinator.RequestTask", &args, &reply) {
		return reply.Task
	}

	// If call fails, return NoTask
	return Task{Type: NoTask}
}

// reportTask reports task completion to the coordinator
func reportTask(workerID int, task Task, success bool) {
	args := ReportTaskArgs{
		WorkerID: workerID,
		Task:     task,
		Success:  success,
	}
	reply := ReportTaskReply{}

	call("Coordinator.ReportTask", &args, &reply)
}

// executeMapTask executes a map task
func executeMapTask(task Task, mapf func(string, string) []KeyValue) bool {
	// Read input file
	filename := task.InputFiles[0]
	file, err := os.Open(filename)
	if err != nil {
		log.Printf("cannot open %v", filename)
		return false
	}
	defer file.Close()

	content, err := os.ReadFile(filename)
	if err != nil {
		log.Printf("cannot read %v", filename)
		return false
	}

	// Call Map function
	kva := mapf(filename, string(content))

	// Create intermediate files for each reduce task
	intermediate := make([][]KeyValue, task.NReduce)
	for _, kv := range kva {
		reduceIndex := ihash(kv.Key) % task.NReduce
		intermediate[reduceIndex] = append(intermediate[reduceIndex], kv)
	}

	// Write intermediate files
	for reduceIndex, kvs := range intermediate {
		intermediateFile := fmt.Sprintf("mr-%d-%d", task.Index, reduceIndex)
		// Always create the file, even if empty
		if !writeKeyValuesToFile(intermediateFile, kvs) {
			return false
		}
	}

	return true
}

// executeReduceTask executes a reduce task
func executeReduceTask(task Task, reducef func(string, []string) string) bool {
	// Collect all intermediate key-value pairs for this reduce task
	var intermediate []KeyValue
	for _, filename := range task.InputFiles {
		kvs := readKeyValuesFromFile(filename)
		if kvs == nil {
			return false
		}
		intermediate = append(intermediate, kvs...)
	}

	// Sort by key
	sort.Sort(ByKey(intermediate))

	// Create output file
	outputFile := fmt.Sprintf("mr-out-%d", task.Index)
	tempFile, err := os.CreateTemp(".", "mr-reduce-*")
	if err != nil {
		log.Printf("cannot create temp file")
		return false
	}
	defer os.Remove(tempFile.Name())

	// Call Reduce on each distinct key
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}

		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}

		output := reducef(intermediate[i].Key, values)

		// Write to temp file
		fmt.Fprintf(tempFile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	tempFile.Close()

	// Atomically rename temp file to final output file
	if err := os.Rename(tempFile.Name(), outputFile); err != nil {
		log.Printf("cannot rename temp file to %v", outputFile)
		return false
	}

	return true
}

// writeKeyValuesToFile writes key-value pairs to a file using JSON encoding
func writeKeyValuesToFile(filename string, kvs []KeyValue) bool {
	// Create temp file in the same directory as the target file
	dir := "."
	tempFile, err := os.CreateTemp(dir, "mr-map-*")
	if err != nil {
		log.Printf("cannot create temp file: %v", err)
		return false
	}
	defer os.Remove(tempFile.Name())

	// Use JSON encoding as suggested in the hints
	enc := json.NewEncoder(tempFile)
	for _, kv := range kvs {
		if err := enc.Encode(&kv); err != nil {
			log.Printf("cannot encode key-value pair: %v", err)
			return false
		}
	}

	if err := tempFile.Close(); err != nil {
		log.Printf("cannot close temp file: %v", err)
		return false
	}

	// Atomically rename temp file to final file
	if err := os.Rename(tempFile.Name(), filename); err != nil {
		log.Printf("cannot rename temp file to %v: %v", filename, err)
		return false
	}

	return true
}

// readKeyValuesFromFile reads key-value pairs from a file using JSON decoding
func readKeyValuesFromFile(filename string) []KeyValue {
	file, err := os.Open(filename)
	if err != nil {
		log.Printf("cannot open %v: %v", filename, err)
		return nil
	}
	defer file.Close()

	var kvs []KeyValue
	dec := json.NewDecoder(file)
	for {
		var kv KeyValue
		if err := dec.Decode(&kv); err != nil {
			break
		}
		kvs = append(kvs, kv)
	}

	return kvs
}

// ByKey for sorting by key
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }
