package ubatch

import (
	. "cheyne.nz/ubatch/pkg/ubatch/types"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

type ProcessState int32

const (
	STOPPED  ProcessState = iota
	STARTING ProcessState = iota
	STARTED  ProcessState = iota
	STOPPING ProcessState = iota
)

type MicroBatchProcesses struct {
	periodicBatchLoop ProcessState
}

type MicroBatcher[T, R any] struct {
	config         Config
	BatchProcessor *BatchProcessor[T, R]
	input          InputReceiver[T]
	response       map[Id]Result[R]

	processes MicroBatchProcesses

	// TODO: include in client?
	output chan []Job[T]
	event  eventBus

	log *slog.Logger
}

type event struct{}

type eventBus struct {
	queue chan QueueEvent
	send  chan any
	recv  map[Id]chan any
}

func newEventBus() eventBus {
	return eventBus{
		queue: make(chan QueueEvent),
		send:  make(chan any),
		recv:  make(map[Id]chan any),
	}
}

var (
	ErrNoInput    = errors.New("batch input stopped")
	ErrJobRefused = errors.New("input receiver did not accept request")
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

// TODO: make internal
type QueueEvent struct {
	Size int
}

// TODO: make internal
type ControlState struct {
	state            ProcessState
	stateChange      chan ProcessState
	waitUntilStopped chan bool
	waitUntilStarted chan bool
}

// InputReceiver accepts jobs and queue them for the MicroBatch process
type InputReceiver[T any] struct {
	accept   bool
	muAccept sync.RWMutex
	// The number of pending jobs submitted to the receiver which have not yet been queued
	pending atomic.Int32
	control ControlState
	//running atomic.Bool
	// The receiver buffers incoming Jobs before they are added to the queue
	receiver chan Job[T]
	muQueue  sync.RWMutex

	queue *[]Job[T]
	// TODO: could add a threshold to reduce number of events sent
	queueEventBus *chan<- QueueEvent
	log           *slog.Logger
}

// Submit a job to the InputReceiver
func (input *InputReceiver[T]) Submit(job Job[T]) error {
	input.muAccept.RLock()
	defer input.muAccept.RUnlock()
	if input.accept {
		input.pending.Add(1)
		input.log.Debug("Job Submitted.", "PendingItems", input.pending.Load())
		input.receiver <- job
		return nil
	} else {
		return ErrJobRefused
	}
}

func (input *InputReceiver[T]) EventBus(eb *chan<- QueueEvent) {
	input.queueEventBus = eb
}

// the InputReceiver uses a simple state machine to control its state
// the state transitions are detailed in startControlLoop changes are triggered by the Start and Stop methods
func (input *InputReceiver[T]) startControlLoop() {
	input.updateState(STARTING)
	for {
		select {
		case s := <-input.control.stateChange:
			if s == STARTING {
				input.log.Info("Input receiver starting.")
				input.accept = true
				input.updateState(STARTED)
			}
			if s == STOPPING {
				input.log.Info("Input receiver stopping")
				input.muAccept.Lock()
				input.accept = false
				input.muAccept.Unlock()
				for {
					// Optimistically, pending should be '0' most of the time
					if input.pending.Load() == 0 {
						input.updateState(STOPPED)
						break
					} else {
						time.Sleep(10 * time.Millisecond)
					}
				}
			}
			if s == STARTED {
				input.log.Info("Input receiver started.")
				input.control.waitUntilStarted <- true
			}
			if s == STOPPED {
				input.log.Info("Input receiver stopped")
				input.control.waitUntilStopped <- true
				return
			}
		}
	}
}

// Start signals the InputReceiver can accept jobs and activates the input goroutine
func (input *InputReceiver[T]) Start() {
	if input.control.state == STOPPED {
		go input.startControlLoop()

		go func() {
			for {
				select {
				case job := <-input.receiver:
					input.log.Debug("Job received", "Id", job.Id)
					input.pending.Add(-1)
					input.log.Debug("Pending input", "PendingItems", input.pending.Load())
					input.muQueue.RLock()
					*(input.queue) = append(*(input.queue), job)
					if input.queueEventBus != nil {
						msg := QueueEvent{Size: len(*(input.queue))}
						select {
						case *input.queueEventBus <- msg:
						default:
							input.log.Debug("Skipped sending QueueEvent", "QueueSize", msg.Size)
						}
					}
					input.muQueue.RUnlock()
				}
			}
		}()
		<-input.control.waitUntilStarted
	} else {
		input.log.Error("InputReceiver is already running. Cannot start it twice.")
	}
}

func (input *InputReceiver[T]) updateState(state ProcessState) {
	input.log.Debug("State change", "from", input.control.state, "to", state)
	input.control.state = state
	input.control.stateChange <- state
}

// Stop signals the InputQueue to stop accepting new jobs
func (input *InputReceiver[T]) Stop() {
	if input.control.state == STARTED {
		input.log.Info("Stopping InputReceiver.")
		input.updateState(STOPPING)
		<-input.control.waitUntilStopped
	} else {
		input.log.Error("Cannot stop InputReceiver", "State", input.control.state)
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
		control: ControlState{
			state:            STOPPED,
			stateChange:      make(chan ProcessState, 1),
			waitUntilStopped: make(chan bool, 1),
			waitUntilStarted: make(chan bool, 1),
		},
	}
}

// Shutdown performs graceful shutdown
func (mb *MicroBatcher[T, _]) Shutdown() {
	mb.input.Stop()

	// TODO: close the input channel
}

func NewMicroBatcher[T, R any](conf Config, processor *BatchProcessor[T, R], logger *slog.Logger) MicroBatcher[T, R] {

	if logger == nil {
		// TODO: Similar functionality may be coming soon - see https://github.com/golang/go/issues/62005
		handler := slog.NewTextHandler(io.Discard, nil)
		logger = slog.New(handler)
	}

	newEventBus()

	return MicroBatcher[T, R]{
		config:         conf,
		BatchProcessor: processor,
		input:          NewInputReceiver[T](conf.Input, logger),
		event:          newEventBus(),
		response:       make(map[Id]Result[R]),
		// TODO: output chan can get blocked??
		output: make(chan []Job[T], 1),
		log:    logger,
	}
}

// Run starts the batch processor executing its Algorithm to send micro-batches

func (mb *MicroBatcher[T, R]) Run() {

	mb.input.Start()
	mb.startPeriodicTrigger()
	//go startPeriodicTrigger(mb.config.Batch, mb.event.send, mb.log)

	// Prepare a micro-batch
	sendEventLoop := func() {
		for {
			// TODO: add shutdown send channel signal
			select {
			case <-mb.event.send:
				batch := mb.input.PrepareBatch()
				if len(batch) > 0 {
					mb.log.Debug("Sending to Batch processor")

					// TODO: what happens if the output backs up?
					//       a sensible default could be larger batch sizes?
					mb.output <- batch
				} else {
					mb.log.Debug("Empty Batch")
				}
			}
		}
	}
	go sendEventLoop()

	// output
	// TODO: split function into initSender
	batchClientLoop := func() {
		for {
			select {
			// TODO: add shutdown client channel signal
			case batch, ok := <-mb.output:
				if ok {
					mb.log.Info("Sending jobs to Batch processor", "JobCount", len(batch))
					res := (*(mb.BatchProcessor)).Process(batch)
					for i := 0; i < len(res); i++ {
						id := res[i].Id
						mb.response[id] = res[i]
						mb.unWait(id)
					}
					// TODO call batch processor
				} else {
					// TODO: log that the client has stopped
					return
				}
			}
		}
	}
	go batchClientLoop()
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

// startPeriodicTrigger starts a goroutine which periodically sends a send event to a channel
// func startPeriodicTrigger(conf BatchOptions, send chan<- any, log *slog.Logger) {
func (mb *MicroBatcher[T, R]) startPeriodicTrigger() {
	if mb.processes.periodicBatchLoop != STOPPED {
		mb.log.Error("Cannot start PeriodicBatchLoop", "status", mb.processes.periodicBatchLoop)
		return
	}

	var ticker *time.Ticker = nil
	if mb.config.Batch.Interval > 0 {
		// TODO: can we pretty-print conf.Interval to appropriate units?
		mb.log.Info("Starting periodic micro-batch timer.", "BatchInterval", mb.config.Batch.Interval)
		ticker = time.NewTicker(mb.config.Batch.Interval)
	} else {
		mb.log.Info("Periodic interval not set.")
		return
	}
	mb.processes.periodicBatchLoop = STARTED
	for mb.processes.periodicBatchLoop == STARTED {
		select {
		// TODO: could stop using a channel instead
		//       then we could log the stopping status here
		case <-ticker.C:
			select {
			case mb.event.send <- true:
				mb.log.Debug("Periodic Send event")
			default:
				mb.log.Debug("Periodic Send event skipped")
			}
		}
	}
	mb.log.Info("Periodic Batch Loop Stopped")
	mb.processes.periodicBatchLoop = STOPPED
}

func (mb *MicroBatcher[T, R]) stopPeriodicTrigger() {
	mb.processes.periodicBatchLoop = STOPPING
}
