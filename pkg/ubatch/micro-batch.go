package ubatch

import (
	. "cheyne.nz/ubatch/pkg/ubatch/types"
	"io"
	"log/slog"
	"sync"
	"time"
)

type uEvent struct{}

type uProcesses struct {
	periodic  uProcess
	threshold uProcess
	prepare   uProcess
	send      uProcess
}

type MicroBatcher[T, R any] struct {
	config         Config
	BatchProcessor *BatchProcessor[T, R]
	input          InputReceiver[T]

	// TODO: would a sync.Map be better? We would want to measure the performance
	response   map[Id]Result[R]
	responseMu sync.Mutex

	control uProcesses

	output chan []Job[T]
	event  eventChannels

	log *slog.Logger
}

type eventChannels struct {
	// TODO: Change sent event type to a union of known send types?
	send chan any
	// recv contains a mapping from Job.Id to a unique channel
	recv sync.Map
}

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

// unWait is used to signal to the Submit method that the Result is available in the response map given Job ID
func (mb *MicroBatcher[_, _]) unWait(id Id) {
	recv, ok := mb.event.recv.Load(id)
	if ok {
		recv.(chan any) <- uEvent{}
	} else {
		mb.log.Error("could not load recv channel", "JobId", id)
	}
}

// Shutdown performs graceful shutdown
// This involves:
// * stopping the input receiver from receiving further jobs
// * batching up the remaining items from the input queue
// * sending these batches to the batch processor
func (mb *MicroBatcher[_, _]) Shutdown() {
	mb.input.Stop()
	mb.input.WaitForPending()

	// TODO: if there is a batch size limit, then we may need multiple batches
	// TODO: we could split it in the MicroBatcher or the InputReceiver
	outstanding := mb.input.PrepareBatch()

	// input.queue should be 0 following PrepareBatch, these event loops can be safely stopped
	// TODO: could add additional guards / checks /tests around process handling
	// If the Interval / Threshold were not set in the config, the periodic and threshold processes won't have been started
	mb.control.periodic.StopIfStarted()
	mb.control.threshold.StopIfStarted()
	mb.control.prepare.Stop()

	// call sendBatch directly bypasses the event loops
	// if the batchProcessor is not thread-safe then this would be a problem
	// TODO: consider how best to coordinate out output channel
	mb.output <- outstanding

	// FIXME: there could possibly be a rare race condition
	// consider how best to coordinate the shutdown of the prepare and send event loops
	// mb.processes.send.Stop()
	// TODO: wait until all receive before

}

func (mb *MicroBatcher[_, R]) addResult(result Result[R]) {
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

		control: uProcesses{
			periodic:  newUProcess(),
			threshold: newUProcess(),
			prepare:   newUProcess(),
			send:      newUProcess(),
		},

		event: eventChannels{
			send: make(chan any),
		},
		// the response channel handles the results returned from the BatchProcessor
		response: make(map[Id]Result[R]),

		// the output channel buffers the jobs being sent to the BatchProcessor
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

// sendBatch sends a batch of Jobs to the BatchProcessor
// When the BatchProcessor returns, the results are added to the response map and the associated Submit jobs are
// notified that there is a result available via the recv channel for that ID.
func (mb *MicroBatcher[T, _]) sendBatch(batch []Job[T]) {
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
	periodicTriggerProc := &mb.control.periodic

	if state := periodicTriggerProc.state; state != STOPPED {
		mb.log.Error("Cannot started PeriodicBatchLoop", "state", state)
		return
	}

	if mb.config.Batch.Interval <= 0 {
		mb.log.Info("Periodic Interval not set. Periodic Batch Loop will not be started.")
		return
	}
	// TODO: can we pretty-print the Batch Interval with appropriate Human Readable units?
	mb.log.Info("Periodic Batch Loop Timer.", "BatchInterval", mb.config.Batch.Interval)
	ticker := time.NewTicker(mb.config.Batch.Interval)

	periodicTriggerProc.resetAndMarkStarting()
	for {
		select {
		case s := <-periodicTriggerProc.change:
			switch s {
			case STOPPED:
				mb.log.Info("Periodic Batch Loop Stopped")
				return
			case STARTING:
				periodicTriggerProc.markStarted()
			case STARTED:
				mb.log.Info("Periodic Batch Loop Started")
			case STOPPING:
				mb.log.Debug("Periodic Batch Loop Stopping")
				periodicTriggerProc.markStopped()
			}
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
	if state := mb.control.threshold.state; state != STOPPED {
		mb.log.Error("Cannot started Threshold Event Loop", "state", state)
		return
	}
	if mb.input.queueThreshold <= 0 {
		mb.log.Info("Queue Threshold not set. Threshold Event Loop will not be started.")
		return
	}

	thresholdProc := &mb.control.threshold
	thresholdProc.resetAndMarkStarting()
	for {
		select {
		case s := <-thresholdProc.change:
			switch s {
			case STOPPED:
				mb.log.Info("Threshold Event Loop Stopped")
				return
			case STARTING:
				thresholdProc.markStarted()
			case STARTED:
				mb.log.Info("Threshold Event Loop Started")
			case STOPPING:
				mb.log.Debug("Threshold Event Loop Stopping")
				thresholdProc.markStopped()
			}
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

func (mb *MicroBatcher[_, _]) startPrepareBatchEventLoop() {
	procBatchProc := &mb.control.prepare
	procBatchProc.resetAndMarkStarting()
	for {
		select {
		case s := <-procBatchProc.change:
			switch s {
			case STOPPED:
				mb.log.Info("Prepare Batch Event Loop Stopped")
				return
			case STARTING:
				procBatchProc.markStarted()
			case STARTED:
				mb.log.Info("Prepare Batch Event Loop Started")
			case STOPPING:
				mb.log.Debug("Prepare Batch Event Loop Stopping")
				procBatchProc.markStopped()
			}
		case <-mb.event.send:
			batch := mb.input.PrepareBatch()
			if len(batch) > 0 {
				mb.log.Debug("Sending to Batch Processor Client")

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
	sendProc := &mb.control.send
	sendProc.resetAndMarkStarting()
	for {
		select {
		case s := <-sendProc.change:
			switch s {
			case STOPPED:
				mb.log.Info("Send Event Loop Stopped")
				return
			case STARTING:
				sendProc.markStarted()
			case STARTED:
				mb.log.Info("Send Event Loop Started")
			case STOPPING:
				mb.log.Debug("Send Event Loop Stopping")
				sendProc.markStopped()
			}
		case batch := <-mb.output:
			mb.sendBatch(batch)
		}
	}
}
