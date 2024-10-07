package ubatch

import (
	. "cheyne.nz/ubatch/pkg/ubatch/types"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type MicroBatcher[T, R any] struct {
	config         Config
	BatchProcessor *BatchProcessor[T, R]
	input          InputReceiver[T]
	response       map[Id]Result[R]

	// todo, include in client
	output chan []Job[T]
	event  eventBus
}

type event struct{}

type eventBus struct {
	send chan any
	recv map[Id]chan any
}

func newEventBus() eventBus {
	return eventBus{
		make(chan any),
		make(map[Id]chan any),
	}
}

var (
	ErrNoInput = errors.New("Batch Input Stopped")
)

// Wait blocks until a response signal is sent
func (mb *MicroBatcher[_, T]) wait(id Id) Result[T] {
	mb.event.recv[id] = make(chan any, 1)
	//select {
	//case <-mb.event.recv[id]:
	<-mb.event.recv[id]

	fmt.Println("response recv'd")
	delete(mb.event.recv, id)
	res := mb.response[id]
	delete(mb.response, id)
	return res
	// TODO: could add a timeout
	//}
}

// UnWait is used to signal that a response is available
func (mb *MicroBatcher[_, _]) unWait(id Id) {
	mb.event.recv[id] <- event{}
}

// InputReceiver accepts jobs and queue them for the MicroBatch process
type InputReceiver[T any] struct {
	accept bool
	// The number of pending jobs submitted to the receiver which have not yet been queued
	pending  atomic.Int32
	running  atomic.Bool
	receiver chan Job[T]
	// TODO: if we cannot drain 1 queue in the batch time, we will hit a problem
	//       consider likelihood and mitigations
	muQueue sync.RWMutex
	queue   *[]Job[T]
}

// Submit a job to the InputReceiver
func (input *InputReceiver[T]) Submit(job Job[T]) {
	input.pending.Add(1)
	fmt.Fprintf(os.Stdout, "pending in is %d\n", input.pending.Load())
	input.receiver <- job
}

// Start signals the InputReceiver can accept jobs and activates the input goroutine
func (input *InputReceiver[T]) Start() {
	if input.running.CompareAndSwap(false, true) {
		go func() {
			defer input.running.Store(false)
			input.accept = true
			fmt.Fprintf(os.Stdout, "starting input\n")
			for {
				select {
				case job, ok := <-input.receiver:
					fmt.Fprintf(os.Stdout, "input recieved %d\n", job.Id)
					if !ok {
						// TODO: log we are shutting down input queue
						// TODO: do we clear the pending?
						fmt.Fprintf(os.Stdout, "input channel closed\n")
						return
					} else {
						input.pending.Add(-1)
						fmt.Fprintf(os.Stdout, "pending is %d\n", input.pending.Load())
						input.muQueue.RLock()
						*(input.queue) = append(*(input.queue), job)
						input.muQueue.RUnlock()
					}
				}
			}
		}()
	} else {

		// TODO: log a Error, trying to start something twice
		// and then do nothing
	}
}

// initInputReceiver creates a new Receiver
//func (mb *MicroBatcher[T, _]) initInputReceiver() {
//	mb.input = newInputReceiver[T](mb.config.Input)
//}

// Stop signals the InputQueue to stop accepting new jobs
func (input *InputReceiver[T]) Stop() {
	if input.running.Load() {
		input.accept = false
		close(input.receiver)
	} else {
		// log an Error, cannot stop something that is not running
	}
}

// PrepareBatch creates a batch with all items from the InputReceiver and resets it to an empty state
func (input *InputReceiver[T]) PrepareBatch() []Job[T] {
	// FIXME:  test / fix highWater
	// Set the input queue capacity to the highest value seen
	// There are other ways to sizing this, this approach minimizes resizing but will use more memory on average
	highWater := cap(*(input.queue))
	emtpy := make([]Job[T], 0, highWater)

	input.muQueue.Lock()
	batch := *input.queue
	input.queue = &emtpy
	input.muQueue.Unlock()

	return batch
}

func newInputReceiver[T any](opts InputOptions) InputReceiver[T] {
	queue := make([]Job[T], 0, opts.Queue.Size)
	return InputReceiver[T]{
		receiver: make(chan Job[T], opts.Channel.Size),
		queue:    &queue,
	}
}

// Shutdown performs graceful shutdown
func (mb *MicroBatcher[T, _]) Shutdown() {
	mb.input.Stop()

	// TODO: close the input channel
}

func NewMicroBatcher[T, R any](conf Config, processor *BatchProcessor[T, R]) MicroBatcher[T, R] {
	return MicroBatcher[T, R]{
		config:         conf,
		BatchProcessor: processor,
		input:          newInputReceiver[T](conf.Input),
		event:          newEventBus(),
		response:       make(map[Id]Result[R]),
		// TODO: output chan can get blocked??
		output: make(chan []Job[T], 1),
	}
}

// Run starts the batch processor executing its Algorithm to send micro-batches
func (mb *MicroBatcher[T, R]) Run() {

	mb.input.Start()

	// start MicroBatch algorithm
	var ticker *time.Ticker = nil
	if mb.config.Batch.Interval > 0 {
		ticker = time.NewTicker(mb.config.Batch.Interval)
		go func() {
			for {
				select {
				case <-ticker.C:
					//fmt.Fprintf(os.Stdout, "triggering batch\n")
					select {
					case mb.event.send <- true:
						fmt.Fprintf(os.Stdout, "send event\n")
					default:
						fmt.Fprintf(os.Stdout, "send blocked\n")
					}
				}
			}
		}()
	} else {
		fmt.Fprintf(os.Stdout, "no batch time\n")
	}

	// Prepare a micro-batch
	// TODO: split function into initBatcher
	go func() {
		for {
			select {
			case <-mb.event.send:
				batch := mb.input.PrepareBatch()
				if len(batch) > 0 {
					fmt.Fprintf(os.Stdout, "send batch\n")
					mb.output <- batch
				} else {
					fmt.Fprintf(os.Stdout, "no batch\n")
				}
			}
		}
	}()

	// output
	// TODO: split function into initSender
	batchClient := func() {
		for {
			select {
			case batch, ok := <-mb.output:
				if ok {

					fmt.Printf("batch contains %d items\n", len(batch))
					res := (*(mb.BatchProcessor)).Process(batch)
					for i := 0; i < len(res); i++ {
						id := res[i].Id
						mb.response[id] = res[i]
						mb.unWait(id)
					}
					// TODO call batch processor
					// FIXME: len of batch is wrong

				} else {
					// TODO: log that the client has stopped
					return
				}
			}
		}
	}
	go batchClient()
}

// Submit adds a job to a micro batch and returns the result when it is available
func (mb *MicroBatcher[T, R]) Submit(job Job[T]) Result[R] {
	// FIXME: reenable the if accept?
	//if mb.input.accept {
	//job := Job[T]{
	//	Id:   mb.id.Next(),
	//	Data: data,
	//}
	mb.input.Submit(job)
	return mb.wait(job.Id)

	//} else {
	//	// TODO: Log a warning, MicroBatcher receiver is not accepting requests, and status of config
	//	return Result[R]{
	//		Err: ErrNoInput,
	//	}
	//}
}
