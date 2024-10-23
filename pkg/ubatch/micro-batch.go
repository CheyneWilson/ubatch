package ubatch

import (
	. "cheyne.nz/ubatch/common/types"
	"cheyne.nz/ubatch/receiver"
	"io"
	"log/slog"
	"sync"
	"time"
)

type uEvent struct{}

type processControls struct {
	stopPeriodic    chan any
	periodicStopped chan any
	stopSend        chan any
	sendStopped     chan any
}

type MicroBatcher[T, R any] struct {
	config         UConfig
	BatchProcessor *BatchProcessor[T, R]
	input          receiver.InputReceiver[Job[T]]

	// TODO: would a sync.Map be better? We would want to measure the performance
	response   map[Id]Result[R]
	responseMu sync.Mutex

	control processControls

	output chan []Job[T]
	event  eventChannels

	log *slog.Logger
}

type eventChannels struct {
	// TODO: Change sent event type to a union of known send types?
	send chan any
	// recv contains a mapping from Job.Id to a unique channel
	recv sync.Map

	// inputThreshold receives an event when the threshold is reached
	inputThreshold chan QueueThresholdEvent
}

// preWait initializes the response channel for a Job/Result pair
//
// This channel must be initialized before either wait() or unWait() is called.
func (mb *MicroBatcher[_, T]) preWait(id Id) {
	c := make(chan any, 1)
	_, loaded := mb.event.recv.LoadOrStore(id, c)
	if loaded {
		// TODO: Handle duplicate ID
		mb.log.Error("Map should be empty for ID", "Id", id)
	}
}

// Wait blocks until a response signal is sent
func (mb *MicroBatcher[_, T]) wait(id Id) Result[T] {
	recv, ok := mb.event.recv.Load(id)
	if ok {
		<-recv.(chan any)
		mb.event.recv.Delete(id)
	} else {
		mb.log.Error("could not load recv channel", "JobId", id)
	}
	mb.responseMu.Lock()
	res := mb.response[id]
	delete(mb.response, id)
	mb.responseMu.Unlock()
	return res
}

// unWait is used to signal to the Submit method that the Result is available in the response map given Job ID
func (mb *MicroBatcher[_, _]) unWait(id Id) {
	recv, ok := mb.event.recv.Load(id)
	if ok {
		mb.log.Debug("loaded recv channel", "JobId", id)
		recv.(chan any) <- uEvent{}
	} else {
		mb.log.Error("could not load recv channel", "JobId", id)
	}
}

// stopPeriodic stops the periodicBatchTriggerLoop if it is running
//
// The periodicBatchTriggerLoop will not have been initially started when if the Batch interval is 0.
func (mb *MicroBatcher[T, R]) stopPeriodic() {
	select {
	case mb.control.stopPeriodic <- true:
		<-mb.control.periodicStopped
	default:
	}
}

// stopSend gracefully shuts down the sendBatchLoop
//
// To ensure that all outstanding jobs are processed, stop or flush the input receiver first.
// See the Shutdown method for details
func (mb *MicroBatcher[_, _]) stopSend() {
	mb.control.stopSend <- true
	<-mb.control.sendStopped
}

// Shutdown performs graceful shutdown
// This involves:
// * stopping the input receiver from receiving further jobs
// * batching up the remaining items from the input queue
// * sending these batches to the batch processor
func (mb *MicroBatcher[_, _]) Shutdown() {
	mb.input.Stop()
	mb.log.Debug("Remaining input items", "Count", mb.input.QueueLen())
	mb.stopPeriodic()
	mb.stopSend()
}

// handleResult adds a result to the response map and signals the result is available
func (mb *MicroBatcher[_, R]) handleResult(result Result[R]) {
	mb.responseMu.Lock()
	mb.response[result.Id] = result
	mb.responseMu.Unlock()
	mb.unWait(result.Id)
}

type QueueThresholdEvent struct {
	queueLength int
}

// inputQueueThreshold returns a function which emits QueueThresholdEvent whenever the queue length exceeds a threshold
//
// It is used as a hook for the input receiver
func inputQueueThreshold(threshold int, evt *chan any) func(queueLength int) {
	return func(queueLength int) {
		if queueLength >= threshold {
			*evt <- QueueThresholdEvent{queueLength}
		}
	}
}

func NewMicroBatcher[T, R any](conf UConfig, processor *BatchProcessor[T, R], logger *slog.Logger) MicroBatcher[T, R] {
	if logger == nil {
		// TODO: Similar functionality may be coming soon - see https://github.com/golang/go/issues/62005
		handler := slog.NewTextHandler(io.Discard, nil)
		logger = slog.New(handler)
	}

	sendChan := make(chan any, 10)
	thresholdReached := inputQueueThreshold(conf.Batch.Threshold, &sendChan)

	return MicroBatcher[T, R]{
		config:         conf,
		BatchProcessor: processor,
		input:          receiver.New[Job[T]](conf.Input, logger, thresholdReached),

		control: processControls{
			stopPeriodic:    make(chan any),
			periodicStopped: make(chan any),
			stopSend:        make(chan any),
			sendStopped:     make(chan any),
		},

		event: eventChannels{
			send: sendChan,
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
	go mb.sendBatchLoop()
	go mb.periodicBatchTriggerLoop()
}

// sendBatch sends a batch of Jobs to the BatchProcessor
// When the BatchProcessor returns, the results are added to the response map and the associated Submit jobs are
// notified that there is a result available via the recv channel for that ID.
func (mb *MicroBatcher[T, _]) sendBatch(batch []Job[T]) {
	if len(batch) > 0 {
		mb.log.Info("Sending jobs to Batch processor", "JobCount", len(batch))
		res := (*mb.BatchProcessor).Process(batch)
		for i := 0; i < len(res); i++ {
			mb.handleResult(res[i])
		}
	} else {
		mb.log.Debug("No batch jobs. Skipping send ")
	}
}

// Submit adds a job to a micro batch and returns the result when it is available
func (mb *MicroBatcher[T, R]) Submit(job Job[T]) Result[R] {
	mb.preWait(job.Id)
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

// periodicBatchTriggerLoop periodically sends a message to the event.send channel to trigger a batch of jobs to be processed
//
// The frequency depends on config.Batch.Interval. If this is 0, then no periodic messages are sent.
func (mb *MicroBatcher[_, _]) periodicBatchTriggerLoop() {
	if mb.config.Batch.Interval > 0 {
		// TODO: can we pretty-print the Batch Interval with appropriate Human Readable units?
		mb.log.Info("Periodic Batch Loop Timer.", "BatchInterval", mb.config.Batch.Interval)
		periodic := time.NewTicker(mb.config.Batch.Interval)
		for {
			select {
			case <-mb.control.stopPeriodic:
				periodic.Stop()
				mb.control.periodicStopped <- true
				return
			case <-periodic.C:
				select {
				case mb.event.send <- true:
				default:
					mb.log.Info("Send already scheduled, skipping periodic")
				}
			}
		}
	} else {
		mb.log.Info("Periodic Interval not set. Periodic Batches will not be sent.")
		return
	}
}

// sendBatchLoop sends micro-batches to the batch processor
//
// This is triggered whenever an event is sent to the event.send channel.
func (mb *MicroBatcher[_, _]) sendBatchLoop() {
	for {
		select {
		case <-mb.control.stopSend:
			mb.log.Info("Stopping Send Batch Loop")
			if mb.input.QueueLen() > 0 {
				batch := mb.input.PrepareBatch()
				mb.sendBatch(batch)
			}
			mb.control.sendStopped <- true
			return
		case <-mb.event.send:
			// don't bother trying to send an empty batch
			if mb.input.QueueLen() > 0 {
				batch := mb.input.PrepareBatch()
				mb.sendBatch(batch)
			}
		}
	}
}
