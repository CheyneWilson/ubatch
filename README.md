# Micro-batching library

TODO: Overview of sections

## Outline

The purpose of the library is to group individual requests (job) into batches. These will be submitted to the downstream
service when either of the following conditions are met
 * A batch size reaches a certain size
 * Periodically, regardless of batch size

# Usage

## Importing

This package has not been published to a package repo. See roadmap for more info

## Batch Processor

This library is designed for use with a Batch Processor which fulfils the following contract.

```go
package types

type Id int

type Result[T any] struct {
	Id  Id
	Ok  T
	Err error
}

type Job[T any] struct {
	Id   Id
	Data T
}

type BatchProcessor[T any, R any] interface {
	Process(jobs []Job[T]) []Result[R]
}
```

## Example Usage

An example of how to use the `MicroBatcher` library is shown below.
The `mock.NewEchoService` returns the `Job` `Data` sent to it as `Result` `OK` message.  

```go
package main

import (
	"cheyne.nz/ubatch/internal/mock"
	"cheyne.nz/ubatch/pkg/ubatch"
	"cheyne.nz/ubatch/pkg/ubatch/types"
	"fmt"
	"log/slog"
)

func main() {
	log := slog.Default()
	batchProcessor := mock.NewEchoService[string](0)
	microBatcher := ubatch.NewMicroBatcher(ubatch.DefaultConfig, &batchProcessor, log)
	microBatcher.Start()
	job := types.Job[string]{Data: "Hello", Id: 1}
	r := microBatcher.Submit(job)
	fmt.Printf("Got result: %+v\n", r)
	microBatcher.Shutdown()
}

```

See further examples [here](example).

# Design / Structure

## Input


## Config

TODO: Explain the config approach


# Testing

## Approach

TODO: Explain why util/perf was created, and the role is serves. This should be evident when the tests are read


# Roadmap

The roadmap of contains possible future enhancements / next steps

## Publish package

There are currently no plans to publish this package. It was created as an exercise and for fun.

# Future enhancements

Below is a list of potential future enhancements

1. **Reconfigurable micro-batcher**. Currently, the config is passed to the constructor. A small enhancement would be
to allow this to be dynamically changed.

// TODO: test for duplicate IDs, need to add this functionality in
// TODO: allow the trigger config to be changed/updated (put this in future scope)
