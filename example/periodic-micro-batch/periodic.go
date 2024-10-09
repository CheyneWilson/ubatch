package main

import (
	"cheyne.nz/ubatch/internal/mock"
	"cheyne.nz/ubatch/pkg/ubatch"
	"cheyne.nz/ubatch/test/util/perf/feeder"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

func main() {
	log := slog.Default()
	batchProcessor := mock.NewEchoService[int](0)
	conf := ubatch.DefaultConfig

	conf.Batch.Threshold = 0
	conf.Batch.Interval = 100 * time.Millisecond

	microBatcher := ubatch.NewMicroBatcher[int, int](conf, &batchProcessor, log)
	microBatcher.Start()
	jobs := feeder.NewSequentialJobFeeder()

	var total atomic.Int32
	var wg sync.WaitGroup

	for i := 0; i < 17; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				job := jobs.Feed()
				r := microBatcher.Submit(job)
				fmt.Printf("Got result: %+v\n", r)
				total.Add(1)
			}
		}()
	}
	wg.Wait()
	microBatcher.Shutdown()
	log.Info("All done :)", "Total Jobs Completed", total.Load())
}
