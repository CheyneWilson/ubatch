package ubatch

import (
	"cheyne.nz/ubatch/receiver"
	"time"
)

// UConfig options for MicroBatcher
type UConfig struct {
	Batch BatchTriggerOptions
	Input receiver.InputOptions
}

// BatchTriggerOptions are used to configuring when MicroBatcher sends new to the BatchProcessor
type BatchTriggerOptions struct {
	// When the input queue length reaches the Threshold, a new micro-batch is created.
	// If this Threshold is 0 or less, the trigger is disabled.
	Threshold int

	// A new micro-batch is created periodically at the specified Interval.
	// If queue length is 0 at the time of triggering then no batch is sent (it would be empty).
	// If this Interval is 0 or less, the trigger is disabled.
	Interval time.Duration
}

// TODO: Revise DefaultConfig when we have more concrete use cases.
//   Note, if we have multiple common use cases then several different defaults Configs could be provided.

// DefaultConfig provides sensible defaults for the MicroBatcher.
var DefaultConfig = UConfig{
	Batch: BatchTriggerOptions{
		// Trigger a micro-batch whenever the input queue reaches this length
		Threshold: 0,
		// Trigger a micro-batch periodically at this Interval if the input queue length is 1 or more.
		Interval: 1 * time.Second,
	},

	Input: receiver.InputOptions{
		// The size of the input receiver channel. Note, the default of 1 should be fine for most scenarios.
		ChannelLength: 1,
		// Default size for the input receiver queue. The queue will grow automatically as necessary.
		QueueLength: 16,
	},
}
