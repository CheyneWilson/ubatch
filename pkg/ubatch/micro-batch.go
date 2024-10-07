package ubatch

import (
	. "cheyne.nz/ubatch/pkg/ubatch/types"
	"errors"
	"io"
	"log/slog"
	"sync"
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

	// TODO: would a sync.Map be better? We would want to measure the performance
	response   map[Id]Result[R]
	responseMu sync.Mutex

	processes MicroBatchProcesses

	// TODO: include in client?
	output chan []Job[T]
	event  eventBus

	log *slog.Logger
}

type event struct{}

// TODO: rename eventBus structf
type eventBus struct {
	queue chan QueueEvent
	send  chan any
	// recv comntains a mapping of Job/Result ID to a channel
	recv sync.Map
}

func newEventBus() eventBus {
	return eventBus{
		queue: make(chan QueueEvent),
		send:  make(chan any),
	}
}

var (
	ErrNoInput         = errors.New("batch input stopped")
	ErrJobNotProcessed = errors.New("job was not processed")
)

// Wait blocks until a response signal is sent
func (mb *MicroBatcher[_, T]) wait(id Id) Result[T] {
	mb.event.recv.Store(id, make(chan any, 1))
	recv, ok := mb.event.recv.Load(id)
	if ok {
		<-recv.(chan any)
		mb.log.Debug("Response received", "Id", id)
		mb.event.recv.Delete(id)
	} else {
		mb.log.Error("could not load recv channel", "JobId", id)
	}
	mb.responseMu.Lock()
	res := mb.response[id]
	delete(mb.response, id)
	mb.responseMu.Unlock()
	return res
	// TODO: could add a timeout
}

// unWait is used to signal to the Submit method that the Result is available in the response map given Job Id
func (mb *MicroBatcher[_, _]) unWait(id Id) {
	recv, ok := mb.event.recv.Load(id)
	if ok {
		recv.(chan any) <- event{}
	} else {
		mb.log.Error("could not load recv channel", "JobId", id)
	}
}

// TODO: make internal
type QueueEvent struct {
	Size int
}

// Shutdown performs graceful shutdown
// This involves:
// * stopping the input receiver from receiving further jobs
// * batching up the remaining items from the input queue
// * sending these batches to the batch processor
// *
func (mb *MicroBatcher[T, R]) Shutdown() {
	mb.input.Stop()
	mb.input.WaitForPending()
	outstanding := mb.input.PrepareBatch()

	// TODO: if there is a batch size limit, then we may need multiple batches
	// TODO: we could split it in the MicroBatcher or the InputReceiver
	mb.submitBatch(outstanding)

	// TODO: shutdown any goroutines in the MicroBatcher
	// TODO: should Shutdown be a sync call (the same as InputReceiver.Stop()) ?

	// TODO: we could also do a hard shutdown as follows
	// aborted := mb.input.PrepareBatch()
	//for _, job := range aborted {
	//	r := Result[R]{
	//		Id:  job.Id,
	//		Err: ErrJobNotProcessed,
	//	}
	//	mb.addResult(r)
	//}
}

func (mb *MicroBatcher[T, R]) addResult(result Result[R]) {
	mb.responseMu.Lock()
	mb.response[result.Id] = result
	mb.responseMu.Unlock()
	mb.unWait(result.Id)
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
// TODO: Rename to Start
func (mb *MicroBatcher[T, R]) Run() {

	mb.input.Start()
	go mb.startPeriodicTrigger()
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
					mb.submitBatch(batch)
				} else {
					// TODO: log that the client has stopped
					return
				}
			}
		}
	}
	go batchClientLoop()
}

// submitBatch sends a batch of Jobs to the BatchProcessor
// When the BatchProcessor returns, the results are added to the response map and the associated Submit jobs are
// notified that there is a result available via the recv channel for that Id.
func (mb *MicroBatcher[T, R]) submitBatch(batch []Job[T]) {
	mb.log.Info("Sending jobs to Batch processor", "JobCount", len(batch))
	res := (*mb.BatchProcessor).Process(batch)
	for i := 0; i < len(res); i++ {
		mb.addResult(res[i])
	}
}

// Submit adds a job to a micro batch and returns the result when it is available
func (mb *MicroBatcher[T, R]) Submit(job Job[T]) Result[R] {
	err := mb.input.Submit(job)
	if err != nil {
		// TODO: Log a warning, MicroBatcher receiver is not accepting requests, and status of config
		return Result[R]{
			Id:  job.Id,
			Err: err,
		}
	} else {
		return mb.wait(job.Id)
	}
}

// startPeriodicTrigger starts a goroutine which periodically sends a send event to a channel
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
