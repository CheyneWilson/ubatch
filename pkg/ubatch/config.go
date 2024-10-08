package ubatch

import "time"

// Config options for MicroBatcher
type Config struct {
	Batch BatchTriggerOptions
	Input InputOptions
}

// BatchTriggerOptions are used to configuring when MicroBatcher sends new to the BatchProcessor
type BatchTriggerOptions struct {
	// When the input queue length reaches the Threshold, a new micro-batch is created.
	// If this Threshold is 0 or less, the trigger is disabled.
	// Threshold int

	// A new micro-batch is created periodically at the specified Interval.
	// If queue length is 0 at the time of triggering then no batch is sent (it would be empty).
	// If this Interval is 0 or less, the trigger is disabled.
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
	// If Threshold is greater than 0, trigger a QueueThresholdEvent when the queue length reaches the Threshold
	Threshold int
}

// TODO: Revise DefaultConfig when we have more concrete use cases.
//   Note, several different default could be provided if we have multiple common use cases.

// DefaultConfig provides sensible defaults for the MicroBatcher.
var DefaultConfig = Config{
	Batch: BatchTriggerOptions{
		// TODO: duplicate setting - create a special QueueOptions for this config, restore Threshold below
		//Threshold: 10,
		Interval: 1 * time.Second,
	},
	Input: InputOptions{
		ChannelOptions{
			1,
		},
		QueueOptions{
			Size: 16,
			// TODO: remove Threshold - see note in BatchTriggerOptions above
			Threshold: 0,
		},
	},
}
