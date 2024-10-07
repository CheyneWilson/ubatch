package feeder

// This package contains some lightweight feeders based on the Gatling concept
// Depending on the feeder, it can be thought of as analogous to an Iterator[T] or Stream[T]
// The feeders in the package provide a subset of the functionality for use with testing the micro-batch library
// See https://docs.gatling.io/reference/script/core/session/feeders/ for more info

import (
	"cheyne.nz/ubatch/pkg/ubatch/types"
	"slices"
	"sync/atomic"
)

// Feeder provides data for a simulation
type Feeder[T any] interface {
	Feed() T
}

type ConstFeeder[T any] struct {
	val T
}

func (f *ConstFeeder[T]) Feed() T {
	return f.val
}

func (f *ConstFeeder[T]) FeedN(n int) []T {
	return slices.Repeat([]T{f.val}, n)
}

type ArrayFeeder[T []any] struct {
	next   int
	data   []T
	repeat bool
}

func (f *ArrayFeeder[T]) feed() T {
	d := f.data[f.next]
	if f.next < len(f.data) {
		f.next += 1
	} else {
		f.next = 0
	}
	return d
}

// SequentialJobFeeder produces jobs with sequential IDs. The Job Data is int value of the ID.
type SequentialJobFeeder[t types.Job[int]] struct {
	prev atomic.Int64
}

func NewSequentialJobFeeder() *SequentialJobFeeder[types.Job[int]] {
	feeder := SequentialJobFeeder[types.Job[int]]{}
	feeder.prev.Swap(-1)
	return &feeder
}

// Feed returns the next value in the sequence. Starts at 0 and increases by 1 for each call
func (f *SequentialJobFeeder[T]) Feed() types.Job[int] {
	n := f.prev.Add(1)
	job := types.Job[int]{
		Id:   types.Id(n),
		Data: int(n),
	}
	return job
}
