package receiver

type InputOptions struct {
	// Size is the BufferSize of the pending channel.
	ChannelSize int
	// Default size for the input receiver queue. The queue will grow automatically as necessary.
	QueueSize int
}

var DefaultConfig = InputOptions{
	// A pending channel 1 should be fine for most scenarios.
	ChannelSize: 1,
	// The default queue. It is resized automatically if too small
	QueueSize: 16,
}
