package ubatch

import (
	. "cheyne.nz/ubatch/pkg/ubatch/types"
	"errors"
	"io"
	"log/slog"
	"sync"
	"time"
)

var (
	ErrJobRefused = errors.New("input receiver did not accept request")
)

// inputReceiverControl contains state of the InputReceiver and signal channels to coordinate processes & actions
type inputReceiverControl struct {
	// the state of the InputReceiver
	state   ProcessState
	receive uProcess
}

type QueueThresholdEvent struct{}

// The InputReceiver supports multiple processes submitting Jobs simultaneously.
// Jobs are transferred from the receiver channel to the queue.
type InputReceiver[T any] struct {
	// if accept is false, any jobs submitted are rejected with a ErrJobRefused
	accept   bool
	muAccept sync.RWMutex

	// pending is the number of incoming Jobs which have not yet processed by receiver
	pending   int
	muPending sync.RWMutex

	control inputReceiverControl

	// The receiver buffers incoming Jobs before they are added to the queue
	receiver chan Job[T]

	queue   *[]Job[T]
	muQueue sync.RWMutex

	// A consumer can subscribe to the queueThresholdEvent channel to be notified when the queueThreshold is reached
	// If queueThreshold is not greater than 0, this behaviour is disabled.
	// If the queueThresholdEvent has not been cleared, subsequent events will be skipped until the channel is cleared
	queueThreshold      int
	queueThresholdEvent chan QueueThresholdEvent
	log                 *slog.Logger
}

// Submit a job to the InputReceiver.
// This method safe for concurrent use by multiple goroutines.
func (input *InputReceiver[T]) Submit(job Job[T]) error {
	input.muAccept.RLock()
	defer input.muAccept.RUnlock()
	if input.accept {
		input.muPending.Lock()
		input.pending += 1
		p := input.pending
		input.muPending.Unlock()
		input.log.Debug("Job Submitted.", "PendingItems", p)
		input.receiver <- job
		return nil
	} else {
		return ErrJobRefused
	}
}

// the InputReceiver uses a simple state machine to processes its state
// the state transitions are detailed in startControlLoop changes are triggered by the Start and Stop methods
func (input *InputReceiver[T]) startInputReceiver() {
	input.control.receive.resetAndMarkStarting()
	for {
		select {
		case s := <-input.control.receive.change:
			switch s {
			case STOPPED:
				input.log.Info("Input receiver stopped.")
				input.control.receive.markStopped()
				return
			case STARTING:
				input.log.Info("Input receiver starting.")
				input.muAccept.Lock()
				input.accept = true
				input.muAccept.Unlock()
				input.control.receive.markStarted()
			case STARTED:
				input.log.Info("Input receiver started.")
			case STOPPING:
				input.log.Info("Input receiver stopping.")
				input.muAccept.Lock()
				input.accept = false
				input.muAccept.Unlock()

				hasPending := true
				for hasPending {
					// Optimistically, pending should be '0' most of the time
					input.muPending.RLock()
					p := input.pending
					input.muPending.RUnlock()
					if p == 0 {
						hasPending = false
						input.control.receive.markStopped()
						input.log.Info("Receive Loop Stopped")
					} else {
						time.Sleep(10 * time.Millisecond)
					}
				}
			}

		case job := <-input.receiver:
			input.log.Debug("Job received", "Id", job.Id)
			input.muPending.Lock()
			input.pending -= 1
			p := input.pending
			input.muPending.Unlock()
			input.log.Debug("Pending input", "PendingItems", p)

			input.muQueue.Lock()
			*input.queue = append(*input.queue, job)
			queueLen := len(*input.queue)
			input.muQueue.Unlock()

			if input.queueThreshold > 0 && queueLen == input.queueThreshold {
				select {
				case input.queueThresholdEvent <- QueueThresholdEvent{}:
					input.log.Debug("QueueThresholdEvent sent")
				default:
					input.log.Debug("QueueThresholdEvent skipped sending")
				}
			}
		}
	}
}

// Start signals the InputReceiver can accept jobs and activates the input goroutine
func (input *InputReceiver[T]) Start() {
	if input.control.state == STOPPED {
		input.control.state = STARTING
		go input.startInputReceiver()
		<-input.control.receive.started
		input.control.state = STARTED
	} else {
		input.log.Error("InputReceiver is already running. Cannot started it twice.")
	}
}

// Stop signals the InputQueue to stopped accepting new jobs
func (input *InputReceiver[T]) Stop() {
	if input.control.state == STARTED {
		input.control.state = STOPPING
		input.control.receive.Stop()
		<-input.control.receive.stopped
		input.control.state = STOPPED
	} else {
		input.log.Error("Cannot stop InputReceiver", "State", input.control.state)
	}
}

// PrepareBatch creates a batch with all items from the InputReceiver and resets it to an empty state
func (input *InputReceiver[T]) PrepareBatch() []Job[T] {
	input.log.Debug("Preparing batch.")

	// Set the input queue capacity to the highest value seen
	// There are other ways to sizing this, this approach minimizes resizing but will use more memory on average
	// TODO: test highWater behaviour
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
		control: inputReceiverControl{
			state:   STOPPED,
			receive: newUProcess(),
		},
		queueThreshold:      opts.Queue.Threshold,
		queueThresholdEvent: make(chan QueueThresholdEvent, 1),
	}
}

// The waitForPending was initially used for testing to ensure all submitted jobs arrived to the queue.
// While this receive should be very, very fast, this wait ensures there are no flakey out-by-one errors.

// WaitForPending returns when where are no more items in pending in the receiver channel.
// Calling Stop, WaitForPending, and PrepareBatch sequentially will ensure all inputted items are returned.
// TODO: We could do a unit test for this by counting the success / Err response to ensure all the numbers add up and
//
//	that nothing goes missing
func (input *InputReceiver[T]) WaitForPending() {
	for {
		input.muPending.RLock()
		p := input.pending
		input.muPending.RUnlock()
		if p == 0 {
			break
		}
		time.Sleep(1 * time.Millisecond)
	}
}
