package mr

import (
	"bufio"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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

	for {
		task, files := mapProcess(mapf)
		args := FinishMapTaskArgs{Task: task, Files: files}
		reply := FinishMapTaskReply{}
		call("Master.FinishMapTask", &args, &reply)
		if reply.IsReduceReady {
			break
		}
	}

	for {
		response := ReduceProcess(reducef)
		if response == -1 {
			break
		}
		args := FinishReduceTaskArgs{response}
		reply := FinishReduceTaskReply{}
		call("Master.FinishReduceTask", &args, &reply)
		if reply.IsFinished {
			return
		}
	}
}

func mapProcess(mapf func(string, string) []KeyValue) (*MapTask, []string) {
	args := MapArgs{}
	reply := MapReply{}
	call("Master.MapTask", &args, &reply)
	if reply.Task == nil {
		return nil, nil
	}
	task := reply.Task
	filename := task.Filename
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v: %v", filename, err)
	}
	file.Close()
	kva := mapf(filename, string(content))
	files, err := createIntermediateFile(task.Number, reply.NumberReducerTasks, kva)
	if err != nil {
		log.Fatalf("error creating intermediate file: %v", err)
	}

	return reply.Task, files
}

func ReduceProcess(reducef func(string, []string) string) (returnedTask int) {
	args := ReduceTaskArgs{}
	reply := ReduceTaskReply{}
	call("Master.ReduceTask", &args, &reply)
	task := reply.Task
	if task == nil {
		return -1
	}
	var files []string
	if task.Files == nil {
		files = make([]string, 0)
	} else {
		files = task.Files
	}

	intermediate := []KeyValue{}
	for _, file := range files {
		f, err := os.Open(file)
		if err != nil {
			log.Fatalf("error opening file %v", err)
		}
		defer f.Close()

		scanner := bufio.NewScanner((f))
		for scanner.Scan() {
			line := scanner.Text()
			var kv KeyValue
			err := json.Unmarshal([]byte(line), &kv)
			if err != nil {
				log.Fatalf("error unmarshalling KeyValue: %v", err)
			}
			intermediate = append(intermediate, kv)
		}

		if err := scanner.Err(); err != nil {
			log.Fatalf("error reading file %v", err)
		}
	}

	sort.Sort(ByKey(intermediate))

	oname := fmt.Sprintf("mr-out-%d", task.Number)
	ofile, _ := ioutil.TempFile("", "prefix")
	defer ofile.Close()

	//escreve no arquivo
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

		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()

	err := os.Rename(ofile.Name(), oname)
	if err != nil {
		log.Fatalf("Error renaming the temp file:", err)
	}

	for _, file := range files {
		err := os.Remove(file)
		if err != nil {
			log.Fatalf("erro removing the map intermediate file %s", file)
		}
	}

	return task.Number
}

func createIntermediateFile(task, numberReducerTaks int, kva []KeyValue) (files []string, err error) {
	intermediate := make(map[int][]KeyValue)

	for _, kv := range kva {
		reducerTask := ihash(kv.Key) % numberReducerTaks
		intermediate[reducerTask] = append(intermediate[reducerTask], kv)
	}

	for reducerTask, kvs := range intermediate {
		fileName := fmt.Sprintf("mr-%d-%d", task, reducerTask)
		file, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE, 0666)
		if err != nil {
			log.Fatalf("Failed to create file: %s", err)
			return nil, err
		}
		defer file.Close()

		enc := json.NewEncoder(file)
		for _, kv := range kvs {
			err = enc.Encode(&kv)
			if err != nil {
				log.Fatalf("Failed to write to file: %s", err)
				return nil, err
			}
		}
		if err = file.Close(); err != nil {
			log.Fatalf("Failed closing the file: %s", err)
			return nil, err
		}

		files = append(files, fileName)
	}

	return files, nil
}

// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
