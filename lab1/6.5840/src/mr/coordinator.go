package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	mu sync.Mutex // Lock to protect shared access

	// Map tasks
	mapTasks    []string     // Input files for map tasks
	mapStatus   []TaskStatus // Status of each map task
	mapAssigned []time.Time  // When each map task was assigned
	mapDone     int          // Number of completed map tasks

	// Reduce tasks
	reduceStatus   []TaskStatus // Status of each reduce task
	reduceAssigned []time.Time  // When each reduce task was assigned
	reduceDone     int          // Number of completed reduce tasks

	nReduce int // Number of reduce tasks

	// Worker tracking
	workerIDCounter int
}

type TaskStatus int

const (
	Idle TaskStatus = iota
	InProgress
	Completed
)

// Your code here -- RPC handlers for the worker to call.

// checkTimeouts periodically checks for tasks that have been assigned
// but not completed within the timeout period (10 seconds)
func (c *Coordinator) checkTimeouts() {
	for {
		time.Sleep(2 * time.Second) // Check every 2 seconds

		c.mu.Lock()

		// Check map tasks
		now := time.Now()
		for i, status := range c.mapStatus {
			if status == InProgress {
				assignedTime := c.mapAssigned[i]
				if now.Sub(assignedTime) > 10*time.Second {
					// Task timeout, reset to Idle
					c.mapStatus[i] = Idle
					c.mapAssigned[i] = time.Time{}
				}
			}
		}

		// Check reduce tasks
		for i, status := range c.reduceStatus {
			if status == InProgress {
				assignedTime := c.reduceAssigned[i]
				if now.Sub(assignedTime) > 10*time.Second {
					// Task timeout, reset to Idle
					c.reduceStatus[i] = Idle
					c.reduceAssigned[i] = time.Time{}
				}
			}
		}

		c.mu.Unlock()
	}
}

// RequestTask RPC handler
func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Assign a new worker ID if needed
	if args.WorkerID == 0 {
		c.workerIDCounter++
		args.WorkerID = c.workerIDCounter
	}

	// First, check for available map tasks
	for i, status := range c.mapStatus {
		if status == Idle {
			// Assign this map task
			c.mapStatus[i] = InProgress
			c.mapAssigned[i] = time.Now()

			reply.Task = Task{
				Type:       MapTask,
				Index:      i,
				InputFiles: []string{c.mapTasks[i]},
				NReduce:    c.nReduce,
			}
			return nil
		}
	}

	// If all map tasks are completed, check for reduce tasks
	if c.mapDone == len(c.mapTasks) {
		for i, status := range c.reduceStatus {
			if status == Idle {
				// Assign this reduce task
				c.reduceStatus[i] = InProgress
				c.reduceAssigned[i] = time.Now()

				// Collect intermediate files for this reduce task
				var inputFiles []string
				for mapIndex := 0; mapIndex < len(c.mapTasks); mapIndex++ {
					inputFiles = append(inputFiles, fmt.Sprintf("mr-%d-%d", mapIndex, i))
				}

				reply.Task = Task{
					Type:       ReduceTask,
					Index:      i,
					InputFiles: inputFiles,
					NReduce:    c.nReduce,
				}
				return nil
			}
		}
	}

	// If all reduce tasks are completed, tell worker to exit
	if c.reduceDone == c.nReduce {
		reply.Task = Task{Type: ExitTask}
		return nil
	}

	// No tasks available at the moment
	reply.Task = Task{Type: NoTask}
	return nil
}

// ReportTask RPC handler
func (c *Coordinator) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	task := args.Task

	if args.Success {
		if task.Type == MapTask {
			if c.mapStatus[task.Index] == InProgress {
				c.mapStatus[task.Index] = Completed
				c.mapDone++
			}
		} else if task.Type == ReduceTask {
			if c.reduceStatus[task.Index] == InProgress {
				c.reduceStatus[task.Index] = Completed
				c.reduceDone++
			}
		}
	} else {
		// Task failed, reset to Idle
		if task.Type == MapTask {
			c.mapStatus[task.Index] = Idle
			c.mapAssigned[task.Index] = time.Time{}
		} else if task.Type == ReduceTask {
			c.reduceStatus[task.Index] = Idle
			c.reduceAssigned[task.Index] = time.Time{}
		}
	}

	reply.Acknowledged = true
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server(sockname string) {
	rpc.Register(c)
	rpc.HandleHTTP()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatalf("listen error %s: %v", sockname, e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Job is done when all reduce tasks are completed
	return c.reduceDone == c.nReduce
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.mapTasks = files
	c.nReduce = nReduce

	// Initialize map tasks
	c.mapStatus = make([]TaskStatus, len(files))
	c.mapAssigned = make([]time.Time, len(files))

	// Initialize reduce tasks
	c.reduceStatus = make([]TaskStatus, nReduce)
	c.reduceAssigned = make([]time.Time, nReduce)

	// Start background goroutine to check for timeouts
	go c.checkTimeouts()

	c.server(sockname)
	return &c
}
