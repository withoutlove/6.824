package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

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

// Add your RPC definitions here.

// Task types
const (
	MapTask = iota
	ReduceTask
	NoTask   // No task available
	ExitTask // Worker should exit
)

// Task definition
type Task struct {
	Type       int      // MapTask or ReduceTask
	Index      int      // Task index
	InputFiles []string // Input files (for Map tasks)
	NReduce    int      // Number of reduce tasks (for Map tasks)
}

// RequestTask RPC arguments
type RequestTaskArgs struct {
	WorkerID int // Worker identifier
}

// RequestTask RPC reply
type RequestTaskReply struct {
	Task Task
}

// ReportTask RPC arguments
type ReportTaskArgs struct {
	WorkerID int
	Task     Task
	Success  bool
}

// ReportTask RPC reply
type ReportTaskReply struct {
	Acknowledged bool
}
