package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"
	"runtime"
)

type IData interface {
	Done() (interface{}, string)
}

type SData struct {
	Name string
	Desc string
	Age int
	Lv int
	Flag string
}

func (d *SData) Done() (interface{}, string) {
	//log.Printf("%s done.\n", d.Name)
	return 0, ""
}

type SJob struct { // todo:可省?
	Data *IData
}
type SWorker struct {
	ID           string
	WorkerBench  chan *SJob
	GWorkerBench chan (chan *SJob)
	Finished     chan bool
}

func (w *SWorker) Start() {
	go func() {
		for {
			w.GWorkerBench <- w.WorkerBench
			select {
			case job := <-w.WorkerBench:
				{
					/*ret, err := */(*job.Data).Done()
					//log.Println(ret, err)
				}
			case bFinished := <-w.Finished:
				{
					if true == bFinished {
						log.Printf("%s finish work.\n", w.ID)
						return
					}
				}
			}
		}
	}()
}
func (w *SWorker) Stop() {
	go func() {
		w.Finished <- true
	}()
}
func NewWorker(ID int, GWorkerBench chan (chan *SJob)) *SWorker {
	log.Printf("%d worker ready\n", ID)
	return &SWorker{
		ID:           "work_"+strconv.Itoa(ID),
		WorkerBench:  make(chan *SJob),
		GWorkerBench: GWorkerBench,
		Finished:     make(chan bool),
	}
}

type SDispatcher struct {
	ID           string
	MaxWorkers   int
	workers      []*SWorker
	EndSignal    chan os.Signal
	Closed       chan bool
	GWorkerBench chan (chan *SJob)
	GJobs        chan *SJob
}

func (d *SDispatcher) Run() {
	for i := 0; i < d.MaxWorkers; i++ {
		worker := NewWorker(i, d.GWorkerBench)
		d.workers = append(d.workers, worker)
		worker.Start()
	}
	go d.Dispatch()
}
func (d *SDispatcher) Dispatch() {
FLAG_SDISPATCHER_DISPATCHER:
	for {
		select {
		case job, ok := <-d.GJobs:
			{
				if true == ok {
					//log.Println("fetch one job from WorkFlow.")
					go func(j *SJob) {
						workBench := <-d.GWorkerBench
						workBench <- j
					}(job)
				} else {
					for _, w := range d.workers {
						w.Stop()
					}
					d.Closed <- true
					break FLAG_SDISPATCHER_DISPATCHER
				}
			}
		case endSignal := <-d.EndSignal:
			{
				log.Printf("WorkFlow[%s] cmd[%v] GJobs[%d] close sended.\n", d.ID, endSignal, len(d.GJobs))
				close(d.GJobs)
			}
		}
	}
}
func NewDispatcher(id string, MaxWorkers, maxWorkBench int) *SDispatcher {
	Closed := make(chan bool)
	EndSignal := make(chan os.Signal)
	GWorkerBench := make(chan (chan *SJob), MaxWorkers)
	GJobs := make(chan *SJob, maxWorkBench)
	signal.Notify(EndSignal, syscall.SIGINT, syscall.SIGTERM)
	return &SDispatcher{
		ID:           id,
		MaxWorkers:   MaxWorkers,
		EndSignal:    EndSignal,
		Closed:       Closed,
		GWorkerBench: GWorkerBench,
		GJobs:        GJobs,
	}
}

type SWorkFlow struct {
	Dispatcher *SDispatcher
}

func (wf *SWorkFlow) Start(MaxWorkers, maxWorkBench int) {
	wf.Dispatcher = NewDispatcher("D1001", MaxWorkers, maxWorkBench)
	wf.Dispatcher.Run()
}
func (wf *SWorkFlow) AddJob(job *SJob) {
	//log.Printf("job:%v, %v, %p, %v\n", *job, job, job, wf.Dispatcher)
	wf.Dispatcher.GJobs <- job
}
func (wf *SWorkFlow) Close() {
	closed := <-wf.Dispatcher.Closed
	if true == closed {
		log.Println("WorkFlow 关闭.")
	}
}


func main() {
	numCpu := runtime.NumCPU()
	runtime.GOMAXPROCS(numCpu - 1)
	var wf SWorkFlow
	wf.Start((numCpu - 1) * 2, 300000)
	go func(wf *SWorkFlow) {
		for i := 0; i < 300000; i++ {
			data := SData{
				fmt.Sprintf("Data_%08d", i+1),
				fmt.Sprintf("Desc_%200d", i+1),
				i+1,
				i*10+1,
				fmt.Sprintf("Flag_%200d", i+1),
			}
			//log.Printf("job:%+v, %d\n", data, len(data.Flag))
			idata := IData(&data)
			job := SJob{
				Data: &idata,
			}
			//log.Printf("job:%v, %p, %p\n", job, job, &job)
			wf.AddJob(&job)
		}
		log.Println("WorkFlow job add over.")
	}(&wf)
	wf.Close()

	time.Sleep(time.Second * 3)
}
