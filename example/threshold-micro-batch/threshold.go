package main

import (
	"cheyne.nz/ubatch/internal/mock"
	"cheyne.nz/ubatch/pkg/ubatch"
	"cheyne.nz/ubatch/test/util/perf/feeder"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"
)

func main() {
	log := slog.Default()
	var batchProcessor = mock.NewEchoService[int](0)
	conf := ubatch.DefaultConfig

	// Batches will be returned
	conf.Batch.Threshold = 8
	conf.Batch.Interval = 0

	microBatcher := ubatch.NewMicroBatcher[int, int](conf, &batchProcessor, log)
	microBatcher.Start()
	jobs := feeder.NewSequentialJobFeeder()

	var total atomic.Int32

	for i := 0; i < 10; i++ {
		go func() {
			for i := 0; i < 10; i++ {
				job := jobs.Feed()
				r := microBatcher.Submit(job)
				fmt.Printf("Got result: %+v\n", r)
				total.Add(1)
			}
		}()
	}
	log.Info("Sleeping for 5 seconds")
	time.Sleep(1 * time.Second)

	log.Info("There should be 4 more results (pending) results returned")
	microBatcher.Shutdown()
	time.Sleep(1 * time.Second)
	log.Info("All done :)", "Total Jobs Completed", total.Load())
}
