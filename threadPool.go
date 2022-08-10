package threadpool

import (
	"fmt"
	"runtime"
	"sync"
	"time"

	. "github.com/AlexandreChamard/go-functor"
	priorityqueue "github.com/AlexandreChamard/go-priorityqueue"
)

type ThreadPool interface {
	Submit(f Functor)
	// Priority: higher value == higher priority
	SubmitPriority(f Functor, priority int)
	// /!\ Does not block, after stopped, use Wait() to wait for all running process to end
	// Wait for all task to be executed
	Stop()
	// /!\ Does not block, after stopped, use Wait() to wait for all running process to end
	// Does not wait for all task to be executed
	ForceStop()
	// Wait for all processes to complete
	Wait()
}

type ThreadPoolConfig struct {
	PoolSize  int
	EnableLog bool
}

// n: maximum number of parellel executions
func NewThreadPool(config ThreadPoolConfig) ThreadPool {
	return makeAndStartThreadPool(config)
}

type State int

const (
	State_ERROR         State = 0
	State_RUNNING       State = 1
	State_WAIT_FOR_STOP State = 2
	State_STOPPED       State = 3
)

type priorityFunctor struct {
	f        Functor
	submitAt time.Time
	priority int
}

func compPriorityFunctor(a, b priorityFunctor) bool {
	if a.priority != b.priority {
		return a.priority > b.priority
	} else {
		return a.submitAt.Before(b.submitAt)
	}
}

type threadPool struct {
	mutex sync.Mutex

	poolSize int
	state    State

	enableLog bool

	pool priorityqueue.PriorityQueue[priorityFunctor]

	lastWorkerId int
	workers      map[int]bool

	taskChan   chan priorityFunctor // send the tasks from the users to manager
	workerChan chan priorityFunctor // send the tasks from the manager to the workers

	forceStop      bool      // true -> ignore all remaining tasks
	stopWorkerChan chan int  // workers send their stop signal to the manager
	stoppedChan    chan bool // force the waiting for the Wait() call
}

func (this *threadPool) Submit(f Functor) {
	if f != nil && this.Running() {
		this.taskChan <- priorityFunctor{f: f, submitAt: time.Now()}
	}
}

func (this *threadPool) SubmitPriority(f Functor, priority int) {
	if f != nil && this.Running() {
		this.taskChan <- priorityFunctor{f: f, submitAt: time.Now(), priority: priority}
	}
}

func (this *threadPool) Stop() {
	this.log(fmt.Sprintf("[threadPool.Stop] START"))
	this.mutex.Lock()
	if this.state != State_RUNNING {
		this.mutex.Unlock()
		return
	}

	this.state = State_WAIT_FOR_STOP
	close(this.taskChan)
	this.mutex.Unlock()

	this.stoppedChan = make(chan bool)
	this.log(fmt.Sprintf("[threadPool.Stop] END"))
}

func (this *threadPool) ForceStop() {
	this.forceStop = true
	this.Stop()
}

func (this *threadPool) Wait() {
	this.log(fmt.Sprintf("[threadPool.Wait] START"))
	<-this.stoppedChan // wait until fully stopped
	this.log(fmt.Sprintf("[threadPool.Wait] END"))
}

func (this *threadPool) Running() bool {
	this.mutex.Lock()
	defer this.mutex.Unlock()
	return this.state == State_RUNNING
}

func (this *threadPool) runningNotSafe() bool {
	return this.state == State_RUNNING
}

type threadPoolWrapper struct {
	*threadPool
}

func makeAndStartThreadPool(config ThreadPoolConfig) *threadPoolWrapper {
	pool := &threadPool{
		poolSize: config.PoolSize,
		state:    State_RUNNING,
		mutex:    sync.Mutex{},

		enableLog: config.EnableLog,

		pool:    priorityqueue.NewPriorityQueue(compPriorityFunctor),
		workers: make(map[int]bool),

		taskChan:   make(chan priorityFunctor),
		workerChan: make(chan priorityFunctor),

		stopWorkerChan: make(chan int),
		stoppedChan:    make(chan bool),
	}

	pool.log(fmt.Sprintf("Start thread pool"))

	for len(pool.workers) < pool.poolSize {
		pool.startWorker()
	}

	go func(this *threadPool) {
	thread_pool_loop:
		for this.runningNotSafe() {

			this.log(fmt.Sprintf("thread pool: wait for action"))

			if this.pool.Empty() {
				select {
				case task, ok := <-this.taskChan:
					{
						if !ok {
							// taskChan is closed -> thread pool must be stopped
							// Error if the thread pool is in a running state
							if this.Running() {
								this.state = State_ERROR
							}
							break thread_pool_loop
						}
						this.pool.Push(task)
						this.log(fmt.Sprintf("new task in the pool (%d)", this.pool.Size()))
					}
				case workerId := <-this.stopWorkerChan:
					{
						this.endWorker(workerId)
					}
				}
			} else {
				select {
				case this.workerChan <- this.pool.Front():
					{
						// A worker took a task
						this.pool.Pop()
					}
				case task, ok := <-this.taskChan:
					{
						if !ok {
							// taskChan is closed -> thread pool must be stopped
							// Error if the thread pool is in a running state
							if this.Running() {
								this.state = State_ERROR
							}
							break thread_pool_loop
						}
						this.pool.Push(task)
						this.log(fmt.Sprintf("new task in the pool (%d)\n", this.pool.Size()))
					}
				case workerId := <-this.stopWorkerChan:
					{
						this.endWorker(workerId)
					}
				}
			}
		}

		// If forceStop is not enable, execute all registered tasks
		for !this.forceStop && !this.pool.Empty() {
			select {
			case this.workerChan <- this.pool.Front():
				{
					// A worker took a task
					this.log(fmt.Sprintf("a worker has taken a task"))
					this.pool.Pop()
				}
			}
		}

		this.log(fmt.Sprintf("close worker chan"))
		this.state = State_STOPPED
		close(this.workerChan)

		// Wait for all workers are closed
		for len(this.workers) > 0 {
			this.log(fmt.Sprintf("wait for worker end (%d)", len(this.workers)))
			workerId := <-this.stopWorkerChan
			this.endWorker(workerId)
		}
		// trigger all Wait()
		close(this.stoppedChan)
	}(pool)

	// Do this trick to not returns the pool. It permits to garbage collect the wrapper and call the Stop function.
	// The pool is self used in its running function so it will never be collected.
	wrapper := &threadPoolWrapper{pool}
	runtime.SetFinalizer(wrapper, func(wrapper *threadPoolWrapper) {
		wrapper.Stop()
	})
	return wrapper
}

func (this *threadPool) startWorker() {
	this.mutex.Lock()
	this.lastWorkerId++
	id := this.lastWorkerId
	this.workers[id] = true
	this.mutex.Unlock()

	this.log(fmt.Sprintf("new worker %d", id))

	go func() {
		// TODO should check if this is worker should be remove (not implemented yet)
	worker_loop:
		for {
			this.log(fmt.Sprintf("%d waits for a task", id))
			select {
			case functor, ok := <-this.workerChan:
				{
					if !ok {
						// Channel workerChan is closed
						break worker_loop
					}
					this.log(fmt.Sprintf("worker %d received a task", id))
					if functor.f != nil {
						functor.f()
					}
				}
			}
		}
		this.log(fmt.Sprintf("worker %d end", id))
		this.stopWorkerChan <- id
	}()
}

func (this *threadPool) endWorker(workerId int) {
	this.log(fmt.Sprintf("worker %d has ended\n", workerId))
	delete(this.workers, workerId)
}

func (t *threadPool) log(s string) {
	if t.enableLog {
		fmt.Println(s)
	}
}
