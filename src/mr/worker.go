package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func execMap(mapf func(string, string) []KeyValue, mapTaskID int, mapParameter1, mapParameter2 string, outputFiles []string) {

	tmpFiles := []*os.File{}
	reduceTasksCount := len(outputFiles)
	for i := 0; i < reduceTasksCount; i++ {
		if tmpFile, err := ioutil.TempFile(intermediateTmpFolder, fmt.Sprintf("tmp-%d-%d", mapTaskID, i)); err != nil {
			log.Fatalf("Fail to create the tmp file at map task %d for reduce task %d with err msg %s.", mapTaskID, i, err.Error())
		} else {
			tmpFiles = append(tmpFiles, tmpFile)
		}
	}

	for _, keyVal := range mapf(mapParameter1, mapParameter2) {
		rid := ihash(keyVal.Key) % reduceTasksCount
		enc := json.NewEncoder(tmpFiles[rid])
		if err := enc.Encode(&keyVal); err != nil {
			log.Fatalf("Fail to write key value %+v at map task %d for reduce task %d with err msg %s", keyVal, mapTaskID, rid, err.Error())
		}
	}

	for rid, tmpFile := range tmpFiles {
		if err := tmpFile.Close(); err != nil {
			log.Fatalf("Fail to close the tmp file at map task %d for reduce task %d with err msg %s", mapTaskID, rid, err.Error())
		}

		intermediateFile := outputFiles[rid]
		if err := os.Rename(tmpFile.Name(), intermediateFile); err != nil {
			log.Fatalf("Fail to rename %s to %s with err msg %s", tmpFile.Name(), intermediateFile, err.Error())
		}
	} // end for
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func execReduce(reducef func(string, []string) string, intermediateFiles []string, outputFile string) {
	var tmpFile *os.File
	var err error
	if tmpFile, err = ioutil.TempFile(intermediateTmpFolder, fmt.Sprintf("tmp-%s", outputFile)); err != nil {
		log.Fatalf("Fail to create the tmp file for reduce output %s with err msg %s", outputFile, err.Error())
	}

	kva := []KeyValue{}
	for _, filename := range intermediateFiles {
		var intermediateFile *os.File
		if intermediateFile, err = os.Open(filename); err != nil {
			log.Fatalf("Fail to open intermediate file %s with err msg %s", filename, err.Error())
		}
		dec := json.NewDecoder(intermediateFile)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
	}

	sort.Sort(ByKey(kva))
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(tmpFile, "%v %v\n", kva[i].Key, output)

		i = j
	}
	if err = tmpFile.Close(); err != nil {
		log.Fatalf("Fail to close the tmp file %s with err msg %s", tmpFile.Name(), err.Error())
	}

	if err := os.Rename(tmpFile.Name(), outputFile); err != nil {
		log.Fatalf("Fail to rename %s to %s with err msg %s", tmpFile.Name(), outputFile, err.Error())
	}
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	// CallExample()
	log.Printf("Start Workder with pid %d\n", os.Getpid())
	for true {
		taskReq := TaskRequest{os.Getpid()}
		taskReply := TaskReply{}

		if status := call("Master.RequestTask", &taskReq, &taskReply); !status {
			log.Println("Master has terminated. Worker exits gracefully. ")
			os.Exit(0)
		} else if taskReply.Cmd == MapOp {
			taskID := 0
			if parsedTaskID, err := strconv.Atoi(taskReply.Args[0]); err != nil {
				log.Fatalf("Fail to parse maptask ID %s with err msg %s", taskReply.Args[0], err.Error())
				taskID = parsedTaskID
			}
			parameter1 := taskReply.Args[1]
			parameter2 := taskReply.Args[2]
			log.Printf("Receive Map Task %d with 1st parameter %s, 1st and last intermediate file (%s, %s) \n", taskID, parameter1, taskReply.OutputFiles[0], taskReply.OutputFiles[len(taskReply.OutputFiles)-1])
			execMap(mapf, taskID, parameter1, parameter2, taskReply.OutputFiles)
			log.Printf("Finish Map Task %d\n", taskID)
		} else if taskReply.Cmd == ReduceOp {
			// taskID := 0
			// if parsedTaskID, err := strconv.Atoi(taskReply.Args[0]); err != nil {
			// 	log.Fatalf("Fail to parse reduce task ID %s with err msg %s", taskReply.Args[0], err.Error())
			// 	taskID = parsedTaskID
			// }
			log.Printf("Receive Reduce Task %s with 1st and last intermediate files (%s, %s), output file %s\n", taskReply.Args[0], taskReply.Args[1], taskReply.Args[len(taskReply.Args)-1], taskReply.OutputFiles[0])

			execReduce(reducef, taskReply.Args[1:], taskReply.OutputFiles[0])
			log.Printf("Finish Reduce Task %s\n", taskReply.Args[0])

		} else if taskReply.Cmd == NoOp {
			time.Sleep(time.Second)
		} else {
			log.Fatalf("Unrecognized cmd %s", taskReply.Cmd.String())
		}
	}
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
