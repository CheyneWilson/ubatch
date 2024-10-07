package ubatch

import (
	. "cheyne.nz/ubatch/pkg/ubatch/types"
	"errors"
	"fmt"
	"io"
	"log/slog"
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
	log     *slog.Logger
}

// Submit a job to the InputReceiver
func (input *InputReceiver[T]) Submit(job Job[T]) {
	input.pending.Add(1)
	input.log.Debug("Job Submitted.", "PendingItems", input.pending.Load())
	input.receiver <- job
}

// Start signals the InputReceiver can accept jobs and activates the input goroutine
func (input *InputReceiver[T]) Start() {
	if input.running.CompareAndSwap(false, true) {
		go func() {
			defer input.running.Store(false)
			input.accept = true
			input.log.Info("Starting input receiver.")
			for {
				select {
				case job, ok := <-input.receiver:
					input.log.Info("Job received", "Id", job.Id)
					if !ok {
						input.log.Info("Input Receiver channel closed")
						// TODO: log we are shutting down input queue
						// TODO: do we clear the pending?
						return
					} else {
						input.pending.Add(-1)
						input.log.Debug("Pending input", "PendingItems", input.pending.Load())
						input.muQueue.RLock()
						*(input.queue) = append(*(input.queue), job)
						input.muQueue.RUnlock()
					}
				}
			}
		}()
	} else {
		input.log.Error("InputReceiver is already running. Cannot start it twice.")
	}
}

// Stop signals the InputQueue to stop accepting new jobs
func (input *InputReceiver[T]) Stop() {
	if input.running.Load() {
		input.log.Info("Stopping InputReceiver.")
		input.accept = false
		close(input.receiver)
	} else {
		input.log.Error("InputReceiver is not running running.")
	}
}

// PrepareBatch creates a batch with all items from the InputReceiver and resets it to an empty state
func (input *InputReceiver[T]) PrepareBatch() []Job[T] {
	input.log.Debug("Preparing batch.")

	// FIXME:  test / fix highWater
	// Set the input queue capacity to the highest value seen
	// There are other ways to sizing this, this approach minimizes resizing but will use more memory on average
	highWater := cap(*(input.queue))
	emtpy := make([]Job[T], 0, highWater)

	input.muQueue.Lock()
	batch := *input.queue
	input.queue = &emtpy
	input.muQueue.Unlock()
	input.log.Debug("Batch prepared.", "BatchSize", len(batch))
	return batch
}

// NewInputReceiver creates a new InputReceiver
//
// logger - optional
func NewInputReceiver[T any](opts InputOptions, logger *slog.Logger) InputReceiver[T] {
	queue := make([]Job[T], 0, opts.Queue.Size)
	if logger == nil {
		// TODO: Similar functionality may be coming soon - see https://github.com/golang/go/issues/62005
		handler := slog.NewTextHandler(io.Discard, nil)
		logger = slog.New(handler)
	}

	return InputReceiver[T]{
		receiver: make(chan Job[T], opts.Channel.Size),
		queue:    &queue,
		log:      logger,
	}
}

// Shutdown performs graceful shutdown
func (mb *MicroBatcher[T, _]) Shutdown() {
	mb.input.Stop()

	// TODO: close the input channel
}

func NewMicroBatcher[T, R any](conf Config, processor *BatchProcessor[T, R]) MicroBatcher[T, R] {
	// TODO: Inject log
	var log *slog.Logger = slog.Default()

	return MicroBatcher[T, R]{
		config:         conf,
		BatchProcessor: processor,
		input:          NewInputReceiver[T](conf.Input, log),
		event:          newEventBus(),
		response:       make(map[Id]Result[R]),
		// TODO: output chan can get blocked??
		output: make(chan []Job[T], 1),
	}
}

// Run starts the batch processor executing its Algorithm to send micro-batches
func (mb *MicroBatcher[T, R]) Run() {

	mb.input.Start()

	startPeriodicBatches := func(conf BatchOptions) {

		var ticker *time.Ticker = nil
		if mb.config.Batch.Interval > 0 {
			fmt.Fprintf(os.Stdout, "starting with periodic time\n")
			ticker = time.NewTicker(conf.Interval)
		} else {
			fmt.Fprintf(os.Stdout, "no batch time\n")
		}
		for {
			select {
			case <-ticker.C:
				select {
				case mb.event.send <- true:
					fmt.Fprintf(os.Stdout, "send event\n")
				default:
					fmt.Fprintf(os.Stdout, "send skipped\n")
				}
			}
		}
	}

	go startPeriodicBatches(mb.config.Batch)

	// Prepare a micro-batch
	processSendEvent := func() {
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
	}
	go processSendEvent()

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
