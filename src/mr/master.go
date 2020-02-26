package mr

import (
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"
)

type TaskStatus int

const (
	Unassigned = iota
	Assigned
	Finished
)

func (s TaskStatus) String() string {
	return [...]string{"Unassigned", "Assigned", "Finished"}[s]
}

type TaskType int

const (
	Map = iota
	Reduce
)

func (d TaskType) String() string {
	return [...]string{"Map", "Reduce"}[d]
}

type TaskInfo struct {
	// taskType   TaskType
	taskID     int
	status     TaskStatus
	timestamp  time.Time // only useful if status = assigned.
	parameters []string
}

type Master struct {
	// Your definitions here.
	phase       TaskType
	mapTasks    map[string]*TaskInfo
	reduceTasks map[string]*TaskInfo
	done        bool
	startTS     time.Time
	mux         sync.Mutex
}

func TaskID(taskType TaskType, id int) string {
	return fmt.Sprintf("%s-%d", taskType.String(), id)
}

const intermediateTmpFolder = "intermediate"

func intermediateFile(mapID, reduceID int) string {
	return filepath.Join(intermediateTmpFolder, fmt.Sprintf("mr-%d-%d", mapID, reduceID))
}

func intermediateFilesByMap(mapID, reduceTasksCount int) (files []string) {
	for rid := 0; rid < reduceTasksCount; rid++ {
		files = append(files, intermediateFile(mapID, rid))
	}
	return
}

func intermediateFilesByReduce(reduceID, mapTasksCount int) (files []string) {
	for mid := 0; mid < mapTasksCount; mid++ {
		files = append(files, intermediateFile(mid, reduceID))
	}
	return
}

func reduceOutputFile(reduceID int) string {
	return fmt.Sprintf("mr-out-%d", reduceID)
}

func mapTaskFinished(mapID, reduceTasksCount int) bool {
	for _, f := range intermediateFilesByMap(mapID, reduceTasksCount) {
		if _, err := os.Stat(f); os.IsNotExist(err) {
			return false
		}
	}
	return true
}

func reduceTaskFinished(reduceID int) bool {
	if _, err := os.Stat(reduceOutputFile(reduceID)); os.IsNotExist(err) {
		return false
	} else {
		return true
	}
}

func (m *Master) RequestTask(args *TaskRequest, reply *TaskReply) error {
	m.mux.Lock()
	defer m.mux.Unlock()
	log.Println("=============================================================")
	log.Printf("Receive Task Request from Worker %d \n", args.WorkPid)
	reduceTasksCount := len(m.reduceTasks)
	mapTaskCount := len(m.mapTasks)
	foundUnassigned := false
	if m.phase == Map {
		for taskName, taskInfo := range m.mapTasks {
			if taskInfo.status == Unassigned {
				foundUnassigned = true
				taskInfo.status = Assigned
				taskInfo.timestamp = time.Now()

				reply.Cmd = MapOp
				// map taskID
				mapTaskID := taskInfo.taskID
				// place the mapTaskID ahead of map parameters
				reply.Args = append(reply.Args, strconv.Itoa(mapTaskID))
				reply.Args = append(reply.Args, taskInfo.parameters...)
				// # of reducers can be inferred from the # of intermediate files
				reply.OutputFiles = intermediateFilesByMap(mapTaskID, reduceTasksCount)
				log.Printf("Reply Worker %d with Task %s\n", args.WorkPid, taskName)
				break
			} // end if
		} // end for
	} else if m.phase == Reduce {
		for taskName, taskInfo := range m.reduceTasks {
			if taskInfo.status == Unassigned {
				foundUnassigned = true
				taskInfo.status = Assigned
				taskInfo.timestamp = time.Now()

				reply.Cmd = ReduceOp
				// map taskID
				reduceTaskID := taskInfo.taskID
				// place the reduceTaskID ahead of reduce parameters
				reply.Args = append(reply.Args, strconv.Itoa(reduceTaskID))
				reply.Args = append(reply.Args, intermediateFilesByReduce(reduceTaskID, mapTaskCount)...)
				// reply.Args = append(reply.Args, m.mapTasks[unassignedTaskID].parameters...)
				// # of reducers can be inferred from the # of intermediate files
				reply.OutputFiles = []string{reduceOutputFile(reduceTaskID)}
				log.Printf("Reply Worker %d with Task %s\n", args.WorkPid, taskName)
				break
			} // end if
		} // end for
	} else {
		log.Fatalf("Unrecognized phase %s", m.phase.String())
	}
	if !foundUnassigned {
		reply.Cmd = NoOp
		log.Printf("Reply Worker %d with NoOp \n", args.WorkPid)
	}
	log.Println("=============================================================")
	return nil
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
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

func (m *Master) CheckStatus() {
	m.mux.Lock()
	defer m.mux.Unlock()
	log.Println("--------------------------------------")
	log.Println("Check status ")

	reduceTasksCount := len(m.reduceTasks)
	allFinished := true
	if m.phase == Map {
		for taskName, taskInfo := range m.mapTasks {
			mapTaskID := taskInfo.taskID

			if taskInfo.status == Assigned {
				if mapTaskFinished(mapTaskID, reduceTasksCount) {
					taskInfo.status = Finished
					taskInfo.timestamp = time.Now()
					fmt.Printf("Finish Task %s at %d ms\n", taskName, time.Since(m.startTS).Milliseconds())
				}
				if time.Since(taskInfo.timestamp).Seconds() > 10 {
					taskInfo.status = Unassigned
					fmt.Printf("Reassign Task %s at %d ms\n", taskName, time.Since(m.startTS).Milliseconds())
				}
			}

			if taskInfo.status != Finished {
				allFinished = false
			}
		} // end for

		if allFinished {
			m.phase = Reduce
		}
	} else if m.phase == Reduce {

		for taskName, taskInfo := range m.reduceTasks {
			reduceTaskID := taskInfo.taskID

			if taskInfo.status == Assigned {
				if reduceTaskFinished(reduceTaskID) {
					taskInfo.status = Finished
					taskInfo.timestamp = time.Now()
					fmt.Printf("Finish Task %s at %d ms\n", taskName, time.Since(m.startTS).Milliseconds())
				}
				if time.Since(taskInfo.timestamp).Seconds() > 10 {
					taskInfo.status = Unassigned
					fmt.Printf("Reassign Task %s at %d ms\n", taskName, time.Since(m.startTS).Milliseconds())
				}
			}

			if taskInfo.status != Finished {
				allFinished = false
			}
		} // end for

		if allFinished {
			m.done = true
		}

	}
	log.Println("--------------------------------------")
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	m.mux.Lock()
	defer m.mux.Unlock()
	ret := m.done

	// Your code here.

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		phase:       Map,
		mapTasks:    map[string]*TaskInfo{},
		reduceTasks: map[string]*TaskInfo{},
		done:        false,
		startTS:     time.Now(),
		mux:         sync.Mutex{},
	}

	// Your code here.
	for mapTaskID, filename := range files {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		mapTaskName := fmt.Sprintf("Map-%d", mapTaskID)
		m.mapTasks[mapTaskName] = &TaskInfo{
			taskID:     mapTaskID,
			status:     Unassigned,
			timestamp:  time.Now(), // This init value is useless
			parameters: []string{filename, string(content)},
		}
	}

	for i := 0; i < nReduce; i++ {
		reduceTaskName := fmt.Sprintf("Reduce-%d", i)
		m.reduceTasks[reduceTaskName] = &TaskInfo{
			taskID:     i,
			status:     Unassigned,
			timestamp:  time.Now(), // this init value is useless
			parameters: []string{}, // useless for reduce task
		}
	}

	log.Println("Start Master Process...")

	// ensure an empty intermediateTmpFolder
	if err := os.RemoveAll(intermediateTmpFolder); err != nil {
		log.Fatalf("Fail to remove directory %s with err msg %s", intermediateTmpFolder, err.Error())
	}
	if err := os.Mkdir(intermediateTmpFolder, os.ModePerm); err != nil {
		log.Fatalf("Fail to create empty directory %s with err msg %s", intermediateTmpFolder, err.Error())
	}

	go func() {
		for {
			m.CheckStatus()
			time.Sleep(time.Second)
		}
	}()

	m.server()
	return &m
}
