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

type uProcess struct {
	state  ProcessState
	stop   chan bool
	change chan ProcessState
}

type MicroBatchProcesses struct {
	periodic  uProcess
	threshold uProcess
	prepare   uProcess
	send      uProcess
	batchRevc uProcess
}

type MicroBatcher[T, R any] struct {
	config         Config
	BatchProcessor *BatchProcessor[T, R]
	input          InputReceiver[T]

	// TODO: would a sync.Map be better? We would want to measure the performance
	response   map[Id]Result[R]
	responseMu sync.Mutex

	batchProcesses MicroBatchProcesses

	// TODO: include in client?
	output chan []Job[T]
	event  eventChannels

	log *slog.Logger
}

type event struct{}

type eventChannels struct {
	// TODO: Change sent event type to a union of known send types?
	send chan any
	// recv contains a mapping from Job.Id to a unique channel
	recv sync.Map
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

	// TODO: add shutdown sequence

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

	return MicroBatcher[T, R]{
		config:         conf,
		BatchProcessor: processor,
		input:          NewInputReceiver[T](conf.Input, logger),
		event: eventChannels{
			send: make(chan any),
		},
		response: make(map[Id]Result[R]),
		// TODO: output chan can get blocked??
		output: make(chan []Job[T], 1),
		log:    logger,
	}
}

// Start the batch processor
func (mb *MicroBatcher[_, _]) Start() {
	mb.input.Start()
	go mb.startPeriodicBatchLoop()
	go mb.startQueueThresholdEventLoop()
	go mb.startPrepareBatchEventLoop()
	go mb.startBatchProcessorClientLoop()

}

// submitBatch sends a batch of Jobs to the BatchProcessor
// When the BatchProcessor returns, the results are added to the response map and the associated Submit jobs are
// notified that there is a result available via the recv channel for that ID.
func (mb *MicroBatcher[T, _]) submitBatch(batch []Job[T]) {
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
		return Result[R]{
			Id:  job.Id,
			Err: err,
		}
	} else {
		return mb.wait(job.Id)
	}
}

// startPeriodicBatchLoop starts a goroutine which periodically sends a send event to a channel
func (mb *MicroBatcher[_, _]) startPeriodicBatchLoop() {

	if state := mb.batchProcesses.periodic.state; state != STOPPED {
		mb.log.Error("Cannot start PeriodicBatchLoop", "state", state)
		return
	}
	mb.batchProcesses.periodic.state = STARTING

	var ticker *time.Ticker = nil
	if mb.config.Batch.Interval > 0 {
		// TODO: can we pretty-print conf.Interval to appropriate units?
		mb.log.Info("Starting periodic micro-batch timer.", "BatchInterval", mb.config.Batch.Interval)
		ticker = time.NewTicker(mb.config.Batch.Interval)
	} else {
		mb.log.Info("Periodic interval not set. Stopping PeriodicBatchLoop.")
		mb.batchProcesses.periodic.state = STOPPED
		return
	}

	// Clear any previous stop signals
	// This protects against a double-stop bug. E.g. the actions Start, Stop, Stop, Start would cause it not to start
	select {
	case <-mb.batchProcesses.periodic.stop:
	default:
	}

	mb.batchProcesses.periodic.state = STARTED
	for {
		select {
		case <-mb.batchProcesses.periodic.stop:
			mb.batchProcesses.periodic.state = STOPPED
			mb.log.Info("Periodic Batch Loop Stopped")
			return
		case <-ticker.C:
			select {
			case mb.event.send <- true:
				mb.log.Debug("Periodic Send event")
			default:
				mb.log.Debug("Periodic Send event skipped")
			}
		}
	}
}

func (mb *MicroBatcher[_, _]) startQueueThresholdEventLoop() {
	if state := mb.batchProcesses.threshold.state; state != STOPPED {
		mb.log.Error("Cannot start MicroBatch Threshold Process", "state", state)
		return
	}
	if mb.input.queueThreshold > 0 {
		mb.batchProcesses.threshold.state = STARTING
		// Clear any previous stop signals
		// This protects against a double-stop bug. E.g. the actions Start, Stop, Stop, Start would cause it not to start
		select {
		case <-mb.batchProcesses.threshold.stop:
		default:
		}
	} else {
		mb.log.Info("Queue Threshold not set. MicroBatch Threshold Process will not be started.")
		return
	}

	mb.batchProcesses.threshold.state = STARTED
	for {
		select {
		case <-mb.batchProcesses.threshold.stop:
			mb.batchProcesses.threshold.state = STOPPED
			mb.log.Info("MicroBatch Threshold Process Stopped")
			return
		case <-mb.input.queueThresholdEvent:
			select {
			case mb.event.send <- true:
				mb.log.Debug("Threshold Event Sent")
			default:
				mb.log.Debug("Threshold Event Skipped")
			}
		}
	}
}

func (mb *MicroBatcher[T, R]) stopPeriodicTrigger() {
	mb.batchProcesses.periodic.stop <- true
}

func (mb *MicroBatcher[_, _]) startPrepareBatchEventLoop() {
	mb.batchProcesses.prepare.state = STARTED
	for {
		select {
		case <-mb.batchProcesses.prepare.stop:
			mb.batchProcesses.prepare.state = STOPPED
			mb.log.Info("MicroBatch Prepare Batch Process Stopped")
			return
		case <-mb.event.send:
			batch := mb.input.PrepareBatch()
			if len(batch) > 0 {
				// FIXME: change this message
				mb.log.Debug("Sending to Batch processor")

				// TODO: if the output channel backs because of a slow BatchProcessor
				//       this causes backpressure causing the PrepareBatchEventLoop to stall
				//       this means a larger batch will be sent when the output channel clears
				// TODO: test this scenario - confirm there is no lag in the process, e.g a small 'next' batch, with the
				//       larger batch sent after that.
				mb.output <- batch
			} else {
				mb.log.Debug("Empty Batch")
			}
		}
	}
}

func (mb *MicroBatcher[_, _]) startBatchProcessorClientLoop() {
	for {
		select {
		case <-mb.batchProcesses.send.stop:
			mb.batchProcesses.send.state = STOPPED
			mb.log.Info("MicroBatch Send Process Stopped")
			return
		case batch := <-mb.output:
			mb.submitBatch(batch)
		}
	}
}
