package mapreduce

import "fmt"

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//

	finishChan := make(chan int, ntasks)
	// treat the serverChan as a ready server pool. when one server finishes its
	// task, put it back to the pool. thus make sure availiable server is busy
	// also need to make sure that jobs are finished
	for i := 0; i < ntasks; i++ {
		go func (whichTask int) {
			thisserver := <- registerChan
			fileArg := ""
			if phase == mapPhase {
				fileArg = mapFiles[whichTask]
			}
			dotaskArgs := DoTaskArgs{jobName, fileArg, phase, whichTask, n_other}
			call(thisserver, "Worker.DoTask", dotaskArgs, nil)
			go func () {registerChan <- thisserver} ()
			finishChan <- whichTask
		} (i)
	}
	// wait for local channel to finish
	for i := 0; i < ntasks; i++ {
		<- finishChan
	}


	fmt.Printf("Schedule: %v done\n", phase)
}
