package receiver

type InputOptions struct {
	ChannelLength int
	QueueLength   int
}

var DefaultConfig = InputOptions{
	// Size is the BufferSize of the channel. Note, the default of 1 should be fine for most scenarios.
	// TODO: test how the performance varies with changing ChannelLength
	ChannelLength: 1,
	// Default size for the input receiver queue. The queue will grow automatically as necessary.
	QueueLength: 16,
}
