package mr

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"
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

	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	CallExample()

	numberOfTries := 0
	for {

		req := JobRequestArgs{1}
		res := JobRequestReply{}

		success := call("Master.RequestJob", &req, &res)

		if success {
			numberOfTries = 0

			if res.TypeOfJob == NoJob {
				return
			} else if res.TypeOfJob == Map {
				processMapJob(&res, mapf)
			} else if res.TypeOfJob == Reduce {
				processReduceJob(&res, reducef)
			}
		} else {
			numberOfTries++
			if numberOfTries > 5 {
				return
			}
		}

		time.Sleep(time.Second)
	}
}

func processMapJob(res *JobRequestReply, mapf func(string, string) []KeyValue) {
	bucketedIntermidates := executeMap(res.NReduce, res.Files, mapf)
	files := saveMap(bucketedIntermidates, res.TaskNumber)

	call("Master.MapJobComplete", &MapJobCompleteArgs{res.TaskNumber, files}, &MapJobCompleteReply{})
}
func executeMap(numOfReduceJob int, filenames []string, mapf func(string, string) []KeyValue) [][]KeyValue {
	var bucketedIntermidates [][]KeyValue

	bucketedIntermidates = make([][]KeyValue, numOfReduceJob)

	intermediate := []KeyValue{}
	for _, filename := range filenames {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		kva := mapf(filename, string(content))
		for _, kv := range kva {
			bucketedIntermidates[ihash(kv.Key)%numOfReduceJob] = append(bucketedIntermidates[ihash(kv.Key)%numOfReduceJob], kv)
		}
		intermediate = append(intermediate, kva...)
	}

	return bucketedIntermidates
}

func saveMap(bucketedIntermidates [][]KeyValue, taskNumber int) []ImmediateFile {
	immediateFiles := []ImmediateFile{}
	fileNames := []string{}
	for bucketID, intermediate := range bucketedIntermidates {
		sort.Sort(ByKey(intermediate))

		oname := "mr-" + strconv.Itoa(taskNumber) + "-" + strconv.Itoa(bucketID)
		ofile, _ := os.Create(oname)
		for _, keyValue := range intermediate {
			// this is the correct format for each line of Reduce output.
			fmt.Fprintf(ofile, "%v %v\n", keyValue.Key, keyValue.Value)
		}
		immediateFiles = append(immediateFiles, ImmediateFile{oname, bucketID})
		fileNames = append(fileNames, oname)
	}

	return immediateFiles
}

func processReduceJob(res *JobRequestReply, reducef func(string, []string) string) {
	augmentedIntermediate := []KeyValue{}

	for _, fileName := range res.Files {
		file, err := os.Open(fileName)
		if err != nil {
			log.Fatal(err)
		}
		defer file.Close()

		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			content := scanner.Text()

			ff := func(r rune) bool { return unicode.IsSpace(r) }

			// split contents into an array of words.
			words := strings.FieldsFunc(content, ff)

			augmentedIntermediate = append(augmentedIntermediate, KeyValue{words[0], words[1]})
		}

		if err := scanner.Err(); err != nil {
			log.Fatal(err)
		}
	}

	sort.Sort(ByKey(augmentedIntermediate))

	oname := "mr-out-" + strconv.Itoa(res.TaskNumber)
	ofile, _ := ioutil.TempFile("./", "temp")

	i := 0
	for i < len(augmentedIntermediate) {
		j := i + 1
		for j < len(augmentedIntermediate) && augmentedIntermediate[j].Key == augmentedIntermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, augmentedIntermediate[k].Value)
		}
		output := reducef(augmentedIntermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", augmentedIntermediate[i].Key, output)

		i = j
	}

	ofile.Close()

	error := os.Rename(ofile.Name(), oname)
	if error != nil {
		log.Fatal(error)
	}
	call("Master.ReduceJobComplete", &ReduceJobCompleteArgs{res.TaskNumber, oname}, &ReduceJobCompleteReply{})

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
