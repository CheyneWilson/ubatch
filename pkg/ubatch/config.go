package ubatch

import "time"

// Config options for MicroBatcher
type Config struct {
	Batch BatchTriggerOptions
	Input InputOptions
}

// BatchTriggerOptions are used to configuring when MicroBatcher sends new to the BatchProcessor
type BatchTriggerOptions struct {
	// A new batch is prepared when the input queue length reaches the Batch Trigger Limit
	// If this Limit is 0 or less, the trigger is disabled
	Limit int
	// A new batch is prepared periodically at the Interval.
	// If this Interval is 0 or less, the trigger is disabled
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
	// Size is the initial Capacity
	Size int
}

// TODO: Revise DefaultConfig when we have more concrete use cases.
//   Note, several different default could be provided if we have multiple common use cases.

// DefaultConfig provides sensible defaults for the MicroBatcher.
var DefaultConfig = Config{
	Batch: BatchTriggerOptions{
		Limit:    10,
		Interval: 1 * time.Second,
	},
	Input: InputOptions{
		ChannelOptions{
			1,
		},
		QueueOptions{
			16,
		},
	},
}
