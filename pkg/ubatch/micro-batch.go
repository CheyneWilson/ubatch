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
	ErrNoInput         = errors.New("batch input stopped")
	ErrJobNotProcessed = errors.New("job was not processed")
)

// Wait blocks until a response signal is sent
func (mb *MicroBatcher[_, T]) wait(id Id) Result[T] {
	mb.event.recv[id] = make(chan any, 1)
	<-mb.event.recv[id]
	mb.log.Debug("Response received", "Id", id)
	delete(mb.event.recv, id)
	mb.responseMu.Lock()
	res := mb.response[id]
	delete(mb.response, id)
	mb.responseMu.Unlock()
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

// Shutdown performs graceful shutdown
func (mb *MicroBatcher[T, R]) Shutdown() {
	mb.input.Stop()
	mb.input.WaitForPending()
	aborted := mb.input.PrepareBatch()
	// TODO: If we supported a gracefully shutdown option, we could send the final batch to the batch processor
	//       This is a possible future feature. Instead, we just error them

	for _, job := range aborted {
		r := Result[R]{
			Id:  job.Id,
			Err: ErrJobNotProcessed,
		}
		mb.addResult(r)
	}

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
					mb.log.Info("Sending jobs to Batch processor", "JobCount", len(batch))
					res := (*(mb.BatchProcessor)).Process(batch)
					for i := 0; i < len(res); i++ {
						//id := res[i].Id
						mb.addResult(res[i])
						//mb.response[id] = res[i]
						//mb.unWait(id)
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
