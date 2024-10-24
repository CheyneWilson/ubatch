package receiver

import (
	"errors"
	"io"
	"log/slog"
	"sync"
)

var (
	ErrJobRefused = errors.New("input receiver did not accept request")
)

type state int32

const (
	STOPPED  state = iota
	STARTING state = iota
	STARTED  state = iota
	STOPPING state = iota
)

// inputReceiverControl contains the state of the InputReceiver and signal channels to coordinate synchronous actions
//
// Sending a message to stopReceiver gracefully shuts down the inputReceiverLoop.
// After the inputReceiverLoop is stopped a message is sent to the receiverStopped channel.
// Sending a message to the flushPending channel triggers all buffered items to be queued.
// After the receiver is flushed a message is sent to the pendingFlushed channel.
type inputReceiverControl struct {
	state           state
	stopReceiver    chan bool
	receiverStopped chan bool
	flushPending    chan bool
	pendingFlushed  chan bool
}

// The InputReceiver supports multiple processes submitting Jobs simultaneously.
// Jobs are transferred from the receiver channel to the queue.
type InputReceiver[T any] struct {
	// if accept is false, any items submitted are rejected with a ErrJobRefused
	accept   bool
	muAccept sync.RWMutex

	// The receiver buffers incoming Jobs before they are added to the queue
	receiver chan T

	// pending is the number of incoming Jobs which have not yet been transferred from receiver to the queue
	pending   int
	muPending sync.RWMutex

	queue   *[]T
	muQueue sync.RWMutex

	control inputReceiverControl

	// onEnqueue hook is called every time a new item is added to the queue
	onEnqueue func(queueLength int)

	log *slog.Logger
}

// Submit an item to the InputReceiver.
// This method safe for concurrent use by multiple goroutines.
func (input *InputReceiver[T]) Submit(item T) error {
	input.muAccept.RLock()
	defer input.muAccept.RUnlock()
	if input.accept {
		input.muPending.Lock()
		input.pending += 1
		input.muPending.Unlock()
		input.receiver <- item
		return nil
	} else {
		return ErrJobRefused
	}
}

// flushPendingItems transfers all pending items from the receiver to the queue
func (input *InputReceiver[T]) flushPendingItems() {
	input.muPending.Lock()
	input.muQueue.Lock()
	for input.pending > 0 {
		input.pending -= 1
		item := <-input.receiver
		*input.queue = append(*input.queue, item)
	}
	input.muQueue.Unlock()
	input.muPending.Unlock()
}

// flushPending is primarily used for testing to checkpoint various states
func (input *InputReceiver[T]) flushPending() {
	input.control.flushPending <- true
	<-input.control.pendingFlushed
}

// inputReceiverLoop transfers pending items to the queue.
func (input *InputReceiver[T]) inputReceiverLoop() {
	input.log.Info("Receive Loop Started")
	for {
		select {
		case <-input.control.flushPending:
			input.flushPendingItems()
			input.control.pendingFlushed <- true
		// Gracefully shutdown
		case <-input.control.stopReceiver:
			input.flushPendingItems()
			input.control.receiverStopped <- true
			input.log.Info("Receive Loop Stopped")
			return
		case item := <-input.receiver:
			input.muPending.Lock()
			input.pending -= 1
			input.muPending.Unlock()
			input.muQueue.Lock()
			*input.queue = append(*input.queue, item)
			qLen := len(*input.queue)
			input.muQueue.Unlock()

			if input.onEnqueue != nil {
				input.onEnqueue(qLen)
			}
		}
	}
}

// Start signals the InputReceiver to accept items and starts the inputReceiverLoop
func (input *InputReceiver[T]) Start() {
	if input.control.state == STOPPED {
		input.control.state = STARTING
		input.accept = true
		go input.inputReceiverLoop()
		input.control.state = STARTED
	} else {
		input.log.Error("InputReceiver is already running. Cannot started it twice.")
	}
}

// Stop prevents the InputQueue from accepting new items
//
// It transfers all pending items to the queue before shutting down the receiverLoop
func (input *InputReceiver[T]) Stop() {
	if input.control.state == STARTED {
		input.control.state = STOPPING
		input.accept = false
		input.control.stopReceiver <- true
		<-input.control.receiverStopped
		input.control.state = STOPPED
	} else {
		input.log.Error("Cannot stop InputReceiver", "State", input.control.state)
	}
}

// PrepareBatch creates a batch with all items from the InputReceiver and resets it to an empty state
func (input *InputReceiver[T]) PrepareBatch() []T {
	input.log.Debug("Preparing batch.")
	input.muQueue.Lock()
	// Set the input queue capacity to the highest value seen
	// There are other ways to sizing this, this approach minimizes resizing but will use more memory on average
	highWater := cap(*(input.queue))
	emtpy := make([]T, 0, highWater)
	batch := *input.queue
	input.queue = &emtpy
	input.muQueue.Unlock()
	return batch
}

// QueueLen returns the current number of items on the queue
func (input *InputReceiver[T]) QueueLen() int {
	input.muQueue.RLock()
	defer input.muQueue.RUnlock()
	return len(*input.queue)
}

// New creates a new InputReceiver
//
// logger - optional
func New[T any](opts InputOptions, logger *slog.Logger, onEnqueue func(queueLength int)) InputReceiver[T] {
	queue := make([]T, 0, opts.QueueLength)
	if logger == nil {
		// TODO: Similar functionality may be coming soon - see https://github.com/golang/go/issues/62005
		handler := slog.NewTextHandler(io.Discard, nil)
		logger = slog.New(handler)
	}

	return InputReceiver[T]{
		receiver: make(chan T, opts.ChannelLength),
		queue:    &queue,
		pending:  0,
		log:      logger,
		control: inputReceiverControl{
			state:           STOPPED,
			flushPending:    make(chan bool),
			pendingFlushed:  make(chan bool),
			stopReceiver:    make(chan bool),
			receiverStopped: make(chan bool),
		},
		onEnqueue: onEnqueue,
	}
}
