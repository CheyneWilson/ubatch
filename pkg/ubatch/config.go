package ubatch

import "time"

// Config options for MicroBatcher
type Config struct {
	Batch BatchOptions
	Input InputOptions
}

// BatchOptions are used to configure the algorithm of the MicroBatcher library
type BatchOptions struct {
	// TODO: remove ExactSize isn't part of the requirements, complicates batch processing unnecessarily
	// ExactSize bool
	// When greater than 0, trigger a micro-batch when the input queue reaches this Limit.
	Limit int
	// When greater than 0, trigger a micro-batch periodically at this Interval.
	Interval time.Duration
}

// InputOptions provide configuration options for the InputReceiver
type InputOptions struct {
	Channel ChannelOptions
	Queue   QueueOptions
}

type ChannelOptions struct {
	// Size is the BufferSize of the channel
	Size int
}

// QueueOptions
type QueueOptions struct {
	// When Resize is true, the Queue will grow when it reaches capacity
	Resize bool
	// Size is the initial Capacity
	Size int
}

// DefaultConfig provides sensible defaults for the MicroBatcher.
//
// TODO: Revise these when we have concrete use cases.
// TODO: Can split into multiple default options if we have multiple primary use cases.
var DefaultConfig = Config{
	Batch: BatchOptions{
		// TODO: remove
		//ExactSize: true,
		Limit:    10,
		Interval: 1 * time.Second,
	},
	Input: InputOptions{
		ChannelOptions{
			1,
		},
		QueueOptions{
			true,
			16,
		},
	},
}
