package mr

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"
)

const checkDuration = time.Second
const taskFinishDur = time.Second * 10

type TaskStatus uint8

const (
	IDLE TaskStatus = iota
	InProgressing
	Finished
)

type ReqType uint8

const (
	AskTask ReqType = iota
	GetCount
	ReportSuccess
	CheckDone
)

type MasterPhase uint8

const (
	MapPhase MasterPhase = iota
	ReducePhase
)

type request struct {
	reqType ReqType
	args    interface{}
	ch      chan interface{}
}
type Master struct {
	// Your definitions here.
	files              []string
	nReduce            int
	nMap               int
	mapStatus          []TaskStatus
	reduceStatus       []TaskStatus
	finishedMapTask    int
	finishedReduceTask int
	mapTimer           []*time.Timer
	reduceTimer        []*time.Timer
	requestCh          chan request
	masterPhase        MasterPhase
	checkTimer         *time.Ticker
}

var errReplyInvalid = errors.New("invaild reply type")

// Your code here -- RPC handlers for the worker to call.

func (m *Master) GetCount(args *EmptyArgs, reply *GetCountReply) error {
	log.Println("master:GetCount")
	ret := m.waitResp(GetCount, args)
	v, ok := ret.(GetCountReply)
	if !ok {
		return errReplyInvalid
	}
	reply.NMap = v.NMap
	reply.NReduce = v.NReduce
	log.Println("master:give nReduce: ", v)
	return nil
}

func (m *Master) AskTask(args *EmptyArgs, reply *Task) error {
	ret := m.waitResp(AskTask, args)
	v, ok := ret.(Task)
	if !ok {
		return errReplyInvalid
	}
	*reply = v
	if reply.Kind == MapTask {
		log.Println("master:give file ", reply.FileName, " to map")
	}
	if reply.Kind == ReduceTask {
		log.Println("master: give task ", reply.TaskId, "to reduce")
	}
	return nil
}

func (m *Master) ReportSuccess(task *Task, success *bool) error {
	ret := m.waitResp(ReportSuccess, task)
	v, ok := ret.(bool)
	if !ok {
		return errReplyInvalid
	}
	*success = v
	log.Println("master:ReportSuccess ", *success)
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (m *Master) realAskTask() Task {
	task := Task{}
	if m.masterPhase == MapPhase {
		var i int
		for i = 0; i < m.nMap; i++ {
			if m.mapStatus[i] == IDLE {
				break
			}
		}
		if i == m.nMap {
			log.Println("master: map task all given, maybe u should sleep for a while")
			task.Kind = Sleep
			return task
		}
		m.mapStatus[i] = InProgressing
		m.mapTimer[i] = time.NewTimer(10 * time.Second)
		task.Kind = MapTask
		task.FileName = m.files[i]
		task.TaskId = i
		return task
	} else {
		// reduce phase
		var i int
		for i = 0; i < m.nReduce; i++ {
			if m.reduceStatus[i] == IDLE {
				break
			}
		}
		if i == m.nReduce {
			log.Println("master: reduce task all given, maybe u should sleep for a while")
			task.Kind = Sleep
			return task
		}
		m.reduceStatus[i] = InProgressing
		m.reduceTimer[i] = time.NewTimer(10 * time.Second)
		task.Kind = ReduceTask
		task.TaskId = i
		return task
	}

}

func (m *Master) realGetCount() GetCountReply {
	count := GetCountReply{NMap: m.nMap, NReduce: m.nReduce}
	return count
}

func (m *Master) realCheckDone() bool {
	return m.finishedMapTask == m.nMap && m.finishedReduceTask == m.nReduce
}

func (m *Master) realReportSuccess(task *Task) bool {
	log.Println("master:realReportSuccess ", *task)
	if m.masterPhase == MapPhase {
		if task.Kind == ReduceTask || m.mapStatus[task.TaskId] != InProgressing {
			return false
		}
		log.Println("mapStatus ", task.TaskId, " change to Finished")
		m.mapStatus[task.TaskId] = Finished
		m.mapTimer[task.TaskId].Stop()
		m.finishedMapTask++
		if m.finishedMapTask == m.nMap {
			log.Println("master change to reduce phase")
			m.masterPhase = ReducePhase
		}
		return true
	} else {
		// in reduce phase
		if task.Kind == MapTask || m.reduceStatus[task.TaskId] != InProgressing {
			return false
		}
		log.Println("reduceStatus ", task.TaskId, " change to Finished")
		m.reduceStatus[task.TaskId] = Finished
		m.reduceTimer[task.TaskId].Stop()
		m.finishedReduceTask++
		log.Println("finishedReduceTask: ", m.finishedReduceTask)
		if m.finishedReduceTask == m.nReduce {
			log.Println("master is ready to end")
		}
		return true
	}
}

// we need the type of the request and corresponding args
// because different request has different args and return, we use empty interface
func (m *Master) waitResp(reqType ReqType, args interface{}) interface{} {
	// log.Println("master: waitResp start")
	ch := make(chan interface{})
	request := request{reqType: reqType, args: args, ch: ch}
	m.requestCh <- request
	// log.Println("master:waitResp: wait eventLoop to handler request")
	return <-ch
}

func (m *Master) eventLoop() {
	for {
		select {
		case req := <-m.requestCh:
			switch req.reqType {
			case AskTask:
				req.ch <- m.realAskTask()
			case GetCount:
				req.ch <- m.realGetCount()
			case ReportSuccess:
				task, _ := (req.args).(*Task)
				req.ch <- m.realReportSuccess(task)
			case CheckDone:
				req.ch <- m.realCheckDone()
			}
		case <-m.checkTimer.C:
			// log.Println("master:eventLoop: one second passed")
			if m.masterPhase == MapPhase {
				for i := range m.mapTimer {
					if m.mapTimer[i] != nil {
						// if over 10s, the channel is not empty
						select {
						case <-m.mapTimer[i].C:
							log.Println("task ", i, " is too late to get donw,fuck it")
							m.mapStatus[i] = IDLE
						default:

						}

					}
				}
			}
			if m.masterPhase == ReducePhase {
				for i := range m.reduceTimer {
					if m.reduceTimer[i] != nil {
						// if over 10s, the channel is not empty
						select {
						case <-m.reduceTimer[i].C:
							log.Println("task ", i, " is too late to get donw,fuck it")
							m.reduceStatus[i] = IDLE
						default:

						}

					}
				}
			}

		}

	}

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

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	args := EmptyArgs{}
	ret := m.waitResp(CheckDone, args)
	res, _ := ret.(bool)
	return res
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		files:              files,
		nReduce:            nReduce,
		nMap:               len(files),
		mapStatus:          make([]TaskStatus, len(files)),
		reduceStatus:       make([]TaskStatus, nReduce),
		finishedMapTask:    0,
		finishedReduceTask: 0,
		mapTimer:           make([]*time.Timer, len(files)),
		reduceTimer:        make([]*time.Timer, nReduce),
		requestCh:          make(chan request),
		masterPhase:        MapPhase,
		checkTimer:         time.NewTicker(checkDuration),
	}

	// Your code here.
	go m.eventLoop()
	m.server()
	return &m
}
