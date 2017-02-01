package mapreduce

import("fmt"
		"sync"
		// "log"
)

// schedule starts and waits for all tasks in the given phase (Map or Reduce).
func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var nios int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)
		nios = mr.nReduce
	case reducePhase:
		ntasks = mr.nReduce
		nios = len(mr.files)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//
	var wg sync.WaitGroup
	for i := 0; i < ntasks;i++ {
		wg.Add(1);
		go func(i int){
			defer wg.Done()
			//another way to create a DoTaskArgs()
			// args := DoTaskArgs{
			// 	JobName:	mr.jobName,
			// 	Phase:		phase,
			// 	TaskNumber: i,
			// 	NumOtherPhase:	nios,
			// }
			
			args := new(DoTaskArgs)
			args.JobName = mr.jobName
			args.Phase = phase
			args.TaskNumber = i
			args.NumOtherPhase = nios
			if phase == mapPhase{
				args.File = mr.files[i]
			}

			//repeat until get a worker to do the task
			for{
				//get the idle worker from the registerChannel
				worker := <-mr.registerChannel
				ok := call(worker,"Worker.DoTask",args,new(struct{}))
				
				if ok==false{
					fmt.Printf("aaaa")
					//break
				}else{
					// log.Println("hehe")
					go func(){
						mr.registerChannel<-worker

					}()
					break
				}
			}
			// fmt.Println("oh yeah")
		}(i)
	}
	wg.Wait()

	fmt.Printf("Schedule: %v phase done\n", phase)
}
