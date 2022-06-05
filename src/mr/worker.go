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
	"time"
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	log.Println("worker start")
	// Your worker implementation here.
	var (
		NReduce int
		NMap    int
	)
	emptyArgs := EmptyArgs{}
	countReply := GetCountReply{}
	call("Master.GetCount", &emptyArgs, &countReply)
	NReduce = countReply.NReduce
	NMap = countReply.NMap
	log.Println("worker:get nReduce ", NReduce)
	log.Println("worker:get nMap ", NMap)
	for {
		task := Task{}
		ret := call("Master.AskTask", &emptyArgs, &task)
		if !ret {
			log.Println("worker:ok, i will fucking exit")
			os.Exit(0)
		}
		switch task.Kind {
		case Sleep:
			log.Println("worker: ok, i will fucking sleep")
			time.Sleep(time.Second)
		case MapTask:
			log.Println("worker: ou yeah, i got file ", task.FileName, " to map")
			file, err := os.Open(task.FileName)
			if err != nil {
				log.Fatalf("cannot open %v", task.FileName)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", task.FileName)
			}
			file.Close()
			kva := mapf(task.FileName, string(content))
			tempFiles := make([]*os.File, NReduce)
			encoders := make([]*json.Encoder, NReduce)
			for i := 0; i < NReduce; i++ {
				tempFiles[i], err = ioutil.TempFile(".", "*")
				encoders[i] = json.NewEncoder(tempFiles[i])
			}
			for i := range kva {
				idx := ihash(kva[i].Key) % NReduce
				encoders[idx].Encode(kva[i])
			}
			success := false
			call("Master.ReportSuccess", &task, &success)
			if success {
				for i := 0; i < NReduce; i++ {
					newFileName := fmt.Sprintf("mr-%v-%v", task.TaskId, i)
					err := os.Rename(tempFiles[i].Name(), newFileName)
					if err != nil {
						log.Fatalln("rename ", tempFiles[i].Name(), "failed!")
					}
					tempFiles[i].Close()
				}
			} else {
				for i := 0; i < NReduce; i++ {
					err := os.Remove(tempFiles[i].Name())
					if err != nil {
						log.Fatalln("remove ", tempFiles[i].Name(), "failed!")
					}
				}
			}
		case ReduceTask:
			log.Println("oh yeah, i got task ", task.TaskId, "to reduce")
			intermediate := []KeyValue{}
			files := make([]*os.File, NMap)
			for i := 0; i < NMap; i++ {
				var err error
				filename := fmt.Sprintf("mr-%v-%v", i, task.TaskId)
				files[i], err = os.Open(filename)
				if err != nil {
					log.Fatalf("cannot open %v", filename)
				}
				dec := json.NewDecoder(files[i])
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					intermediate = append(intermediate, kv)
				}
			}
			sort.Sort(ByKey(intermediate))
			ofile, _ := ioutil.TempFile(".", "*")
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

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

				i = j
			}
			success := false
			call("Master.ReportSuccess", &task, &success)
			if success {
				newName := fmt.Sprintf("mr-out-%v", task.TaskId)
				err := os.Rename(ofile.Name(), newName)
				if err != nil {
					log.Fatalln("rename ", ofile.Name(), "failed!")
				}
				for i := 0; i < NMap; i++ {
					err := os.Remove(files[i].Name())
					if err != nil {
						log.Fatalln("remove ", files[i].Name(), "failed!")
					}
				}
				ofile.Close()
			} else {
				err := os.Remove(ofile.Name())
				if err != nil {
					log.Fatalln("remove ", ofile.Name(), "failed!")
				}
				for i := 0; i < NMap; i++ {
					files[i].Close()
				}
			}
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
