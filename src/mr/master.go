package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const TIME_CHECK_IN_SECONDS = 20

type Master struct {
	MapTasks           *SafeMapTasks
	ReduceTasks        *SafeReduceTasks
	NumberReducerTasks int
	Cond               *Cond
}

type status int

const (
	IDLE status = iota
	IN_PROGRESS
	COMPLETED
)

type Cond struct {
	synchronizer *sync.Cond
	mutex        *sync.Mutex
}

type SafeMapTasks struct {
	mu    sync.Mutex
	tasks map[int]*MapTask
}

type MapTask struct {
	Filename    string
	Status      status
	TimeElapsed time.Time
	Number      int
}

type SafeReduceTasks struct {
	mu    sync.Mutex
	tasks map[int]*ReduceTask
}

type ReduceTask struct {
	Number      int
	Status      status
	TimeElapsed time.Time
	Files       []string
}

func NewSafeMapTasks() *SafeMapTasks {
	return &SafeMapTasks{
		tasks: make(map[int]*MapTask),
	}
}

func NewSafeReduceTasks() *SafeReduceTasks {
	return &SafeReduceTasks{
		tasks: make(map[int]*ReduceTask),
	}
}

// Set safely adds or updates a task in the map.
func (s *SafeMapTasks) Set(id int, task *MapTask) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tasks[id] = task
}

func (s *SafeReduceTasks) Set(id int, task *ReduceTask) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tasks[id] = task
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.

// start a thread that listens for RPCs from worker.go
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
func (m *Master) Done() bool {
	m.ReduceTasks.mu.Lock()
	defer m.ReduceTasks.mu.Unlock()
	for _, task := range m.ReduceTasks.tasks {
		if task.Status != COMPLETED {
			return false
		}

	}

	return true
}

// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	m.NumberReducerTasks = nReduce
	var mu sync.Mutex
	m.Cond = &Cond{}
	m.Cond.mutex = &mu
	m.Cond.synchronizer = sync.NewCond(&mu)
	m.MapTasks = NewSafeMapTasks()
	for index, filename := range files {
		m.MapTasks.Set(index, &MapTask{Filename: filename, Status: IDLE, Number: index})
	}
	m.ReduceTasks = NewSafeReduceTasks()
	for i := 0; i < nReduce; i++ {
		m.ReduceTasks.Set(i, &ReduceTask{Status: IDLE, Files: make([]string, 0), Number: i})
	}
	m.server()
	ticker := time.NewTicker(TIME_CHECK_IN_SECONDS * time.Second)
	go func() {
		for {
			select {
			case <-ticker.C:
				m.checkTasks()
			}
		}
	}()
	return &m
}

func (m *Master) checkTasks() {
	m.MapTasks.mu.Lock()
	defer m.MapTasks.mu.Unlock()
	for i, task := range m.MapTasks.tasks {
		if task.Status == IN_PROGRESS && time.Now().After(task.TimeElapsed.Add(TIME_CHECK_IN_SECONDS*time.Second)) {
			task.Status = IDLE
			task.TimeElapsed = time.Now()
			m.MapTasks.tasks[i] = task
		}
	}
	m.ReduceTasks.mu.Lock()
	defer m.ReduceTasks.mu.Unlock()
	for i, task := range m.ReduceTasks.tasks {
		if task.Status == IN_PROGRESS && time.Now().After(task.TimeElapsed.Add(TIME_CHECK_IN_SECONDS*time.Second)) {
			task.Status = IDLE
			task.TimeElapsed = time.Now()
			m.ReduceTasks.tasks[i] = task
		}
	}
}

func (m *Master) MapTask(args *MapArgs, reply *MapReply) error {
	m.MapTasks.mu.Lock()
	defer m.MapTasks.mu.Unlock()
	for i, task := range m.MapTasks.tasks {
		if task.Status == IDLE {
			reply.Task = task
			reply.NumberReducerTasks = m.NumberReducerTasks
			task.TimeElapsed = time.Now()
			task.Status = IN_PROGRESS
			m.MapTasks.tasks[i] = task
			break
		}
	}
	return nil
}

func (m *Master) FinishMapTask(args *FinishMapTaskArgs, reply *FinishMapTaskReply) error {
	m.MapTasks.mu.Lock()
	defer m.MapTasks.mu.Unlock()
	IsReduceReady := true
	for _, task := range m.MapTasks.tasks {
		if task.Status != COMPLETED {
			IsReduceReady = false
			break
		}
	}
	reply.IsReduceReady = IsReduceReady
	if args.Task == nil {
		return nil
	}
	mapTask := m.MapTasks.tasks[args.Task.Number]
	if mapTask.Status != COMPLETED {
		mapTask.Status = COMPLETED
		for _, file := range args.Files {
			parts := strings.Split(file, "-")
			if len(parts) < 3 {
				log.Fatalln("string is wrong. it doesnt have the expected format")
			}
			reduceTaskStr := parts[len(parts)-1]
			reduceTaskNumber, err := strconv.Atoi(reduceTaskStr)
			if err != nil {
				log.Fatalln("error converting")
			}
			m.ReduceTasks.mu.Lock()
			reduceTask := m.ReduceTasks.tasks[reduceTaskNumber]
			reduceTask.Files = append(reduceTask.Files, file)
			m.ReduceTasks.mu.Unlock()
		}
	}
	return nil
}

func (m *Master) ReduceTask(args *ReduceTaskArgs, reply *ReduceTaskReply) error {
	isReady := true
	m.MapTasks.mu.Lock()
	for _, task := range m.MapTasks.tasks {
		if task.Status != COMPLETED {
			isReady = false
			break
		}
	}
	m.MapTasks.mu.Unlock()
	if !isReady {
		m.Cond.mutex.Lock()
		m.Cond.synchronizer.Wait()
	} else {
		m.Cond.synchronizer.Broadcast()
	}
	m.ReduceTasks.mu.Lock()
	defer m.ReduceTasks.mu.Unlock()
	for i, task := range m.ReduceTasks.tasks {
		if task.Status == IDLE {
			reply.Task = task
			task.Status = IN_PROGRESS
			task.TimeElapsed = time.Now()
			m.ReduceTasks.tasks[i] = task
			break
		}
	}
	fmt.Printf("Task %v\n", reply.Task)
	return nil
}

func (m *Master) FinishReduceTask(args *FinishReduceTaskArgs, reply *FinishReduceTaskReply) error {
	m.ReduceTasks.mu.Lock()
	defer m.ReduceTasks.mu.Unlock()
	task := m.ReduceTasks.tasks[args.Task]
	task.Status = COMPLETED
	isFinished := true
	for _, task := range m.ReduceTasks.tasks {
		if task.Status != COMPLETED {
			isFinished = false
			break
		}
	}
	reply.IsFinished = isFinished

	return nil
}
