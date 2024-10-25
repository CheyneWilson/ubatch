package echo

import (
	. "cheyne.nz/ubatch/types"
	"time"
)

// EchoService is a BatchProcessor that returns results containing the same data as the submitted jobs.
// It has a configurable delay
type EchoService[T any, R any] struct {
	// when delay is greater than 0, delay the Process method before returning
	delay time.Duration
}

// New constructs a new EchoService.
func NewEchoService[T any](delay time.Duration) BatchProcessor[T, T] {
	return &EchoService[T, T]{
		delay: delay,
	}
}

// Process returns a batch of successful results that contain the same data as the input jobs.
func (bp *EchoService[T, R]) Process(jobs []Job[T]) []Result[T] {
	res := make([]Result[T], 0, len(jobs))
	for _, job := range jobs {
		r := Result[T]{
			Id:  job.Id,
			Ok:  job.Data,
			Err: nil,
		}
		res = append(res, r)
	}
	if bp.delay > 0 {
		time.Sleep(bp.delay)
	}
	return res
}
