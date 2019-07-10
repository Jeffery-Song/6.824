package mapreduce

import (
	"encoding/json"
	"os"
	"fmt"
	"sort"
)

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	fmt.Println("the outFile is ", outFile)
	// for each map task, read the kvs
	aggregatedMap := make(map[string][]string)
	for mapTask := 0; mapTask < nMap; mapTask++ {
		inFileName := reduceName(jobName, mapTask, reduceTask)
		inFile, inFileError := os.Open(inFileName)
		if inFileError != nil {
			fmt.Println("An error occurred when openning intermediat file", inFileError)
			return
		}
		defer inFile.Close()

		decoder := json.NewDecoder(inFile)
		var kv *KeyValue
		for {
			decodeError := decoder.Decode(&kv)
			if decodeError != nil {
				break
			}
			if slice, sliceExist := aggregatedMap[kv.Key]; sliceExist {
				aggregatedMap[kv.Key] = append(slice, kv.Value)
			} else {
				aggregatedMap[kv.Key] = make([]string, 1)
				aggregatedMap[kv.Key][0] = kv.Value
			}
		}
	}
	// aggregate values by key
	reducedMap := make(map[string]string)
	for key, values := range aggregatedMap {
		val := reduceF(key, values)
		reducedMap[key] = val
	}

	// sort by key
	keys := make([]string, len(reducedMap))
	i := 0
	for k, _ := range reducedMap {
		keys[i] = k
		i++
	}
	sort.Strings(keys)

	// output to outFile
	oF, outFileError := os.OpenFile(outFile, os.O_WRONLY | os.O_CREATE | os.O_TRUNC, 0666)
	if outFileError != nil {
		fmt.Println("An error occured when openning the outfile", outFileError)
		return
	}
	defer oF.Close()
	encoder := json.NewEncoder(oF)
	for _, k := range keys {
		encoder.Encode(KeyValue{k, reducedMap[k]})
	}


	//
	// doReduce manages one reduce task: it should read the intermediate
	// files for the task, sort the intermediate key/value pairs by key,
	// call the user-defined reduce function (reduceF) for each key, and
	// write reduceF's output to disk.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTask) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	// Your code here (Part I).
	//
}
