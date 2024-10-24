package ubatch

import (
	. "cheyne.nz/ubatch/common/types"
	"github.com/stretchr/testify/assert"
	"internal/feeder"
	"internal/mock/echo-batch-processor"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func configureLogger() *slog.Logger {
	opts := &slog.HandlerOptions{
		// Note: Debug logging can be enabled by simply uncommenting the below line
		// Level: slog.LevelDebug,
	}
	handler := slog.NewTextHandler(os.Stdout, opts)
	return slog.New(handler)
}

var logger = configureLogger()

// TestMicroBatcher_EndToEnd_Single checks that the Job sent to the MicroBatcher returns a Result
func TestMicroBatcher_EndToEnd_Single(t *testing.T) {
	var batchProcessor = echo.NewEchoService[string](0)
	microBatcher := NewMicroBatcher[string, string](DefaultConfig, &batchProcessor, logger)
	microBatcher.Start()
	j := Job[string]{
		Data: "Hello",
		Id:   1,
	}
	r := microBatcher.Submit(j)
	assert.Equal(t, "Hello", r.Ok)
	assert.Nil(t, r.Err)
	microBatcher.Shutdown()
}

func TestMicroBatcher_EndToEnd_Batch(t *testing.T) {
	var batchProcessor = echo.NewEchoService[int](0)
	conf := DefaultConfig
	conf.Batch.Interval = 10 * time.Millisecond
	microBatcher := NewMicroBatcher[int, int](conf, &batchProcessor, logger)
	microBatcher.Start()
	jobs := feeder.NewSequentialJobFeeder()
	for i := 0; i < 10; i++ {
		r := microBatcher.Submit(jobs.Feed())
		assert.Equal(t, i, r.Ok)
		assert.Equal(t, Id(i), r.Id)
		assert.Nil(t, r.Err)
	}
	microBatcher.Shutdown()
}

func TestMicroBatcher_MultiUser_Submit(t *testing.T) {
	var batchProcessor = echo.NewEchoService[int](0)
	conf := DefaultConfig
	//conf.Batch.Interval = 10 * time.Millisecond
	conf.Batch.Interval = 5 * time.Second
	microBatcher := NewMicroBatcher[int, int](conf, &batchProcessor, logger)
	microBatcher.Start()
	jobs := feeder.NewSequentialJobFeeder()

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 10; i++ {
				job := jobs.Feed()
				r := microBatcher.Submit(job)
				assert.Equal(t, int(job.Id), r.Ok)
				assert.Equal(t, job.Id, r.Id)
				assert.Nil(t, r.Err)
			}
		}()
	}
	wg.Wait()
	microBatcher.Shutdown()
	// TODO: add further validation that we got all the results back OK - should be fine but best to double-check
}

// TestMicroBatcher_SingleUser_NoTriggerInterval tests that the micro-batcher does not send jobs to the BatchProcessor
// when the Batch Interval is 0. It also tests that the Result is returned successfully during a graceful Shutdown.
func TestMicroBatcher_SingleUser_NoTriggerInterval(t *testing.T) {
	var batchProcessor = echo.NewEchoService[string](0)

	conf := DefaultConfig
	// TODO: Add a test when Threshold is 0, it should just work
	conf.Batch.Threshold = 10
	conf.Batch.Interval = 0

	microBatcher := NewMicroBatcher[string, string](conf, &batchProcessor, logger)
	microBatcher.Start()

	res := make(chan Result[string], 1)
	go func() {
		j := Job[string]{Id: 1, Data: "Hello"}
		res <- microBatcher.Submit(j)
	}()

	t.Log("Waiting 10 seconds for timeout")
	select {
	case <-res:
		t.Fatalf("Result should not be returned. Interval is 0. No batch should be sumbitted.")
	case <-time.After(10 * time.Second):
		t.Log("Timeout (as expected) after 10 seconds")
		break
	}
	// The micro-batcher should gracefully Shutdown, completing the queued job
	microBatcher.Shutdown()
	r := <-res
	assert.Equal(t, "Hello", r.Ok)
	assert.Equal(t, Id(1), r.Id)
	assert.Nil(t, r.Err)
}

// TestMicroBatcher_SingleUser_Threshold tests that a micro-batch is sent when the input queue Threshold is reached.
func TestMicroBatcher_SingleUser_Threshold(t *testing.T) {
	var batchProcessor = echo.NewEchoService[int](0)

	conf := DefaultConfig
	conf.Batch.Interval = 0
	conf.Batch.Threshold = 5
	microBatcher := NewMicroBatcher[int, int](conf, &batchProcessor, logger)
	microBatcher.Start()

	jobs := feeder.NewSequentialJobFeeder()

	for j := 0; j < 5; j++ {
		var wg sync.WaitGroup
		for i := 0; i < 4; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				job := jobs.Feed()
				r := microBatcher.Submit(job)
				assert.Equal(t, job.Data, r.Ok)
				assert.Equal(t, job.Id, r.Id)
				assert.Nil(t, r.Err)
			}()
		}
		// Note, we don't call wg.Wait() here, because each goroutine will be waiting on it's Submit method call to complete
		t.Log("Waiting for 5 seconds")
		select {
		case <-time.After(5 * time.Second):
			t.Log("Timeout (as expected) after 5 seconds")
			break
		}

		// queue size should equal the number of submitted jobs
		assert.Equal(t, 4, microBatcher.input.QueueLen())

		// Submitting the 5th job causes the input Threshold to be reached, triggering a new micro-batch
		// All outstanding jobs should complete
		t.Log("This job should trigger a new micro-batch")

		completed := make(chan bool, 1)
		go func() {
			job := jobs.Feed()
			r := microBatcher.Submit(job)
			assert.Equal(t, 0, microBatcher.input.QueueLen())
			assert.Equal(t, job.Data, r.Ok)
			assert.Equal(t, job.Id, r.Id)
			assert.Nil(t, r.Err)
			completed <- true
			wg.Wait()
		}()
		select {
		case <-completed:
			break
		case <-time.After(5 * time.Second):
			t.Fatalf("Unexpected timeout - queue threshold trigger did not fire.")
		}

	}
	microBatcher.Shutdown()
}

// TestMicroBatcher_MultiUser_Threshold that a micro-batch is sent when the input queue Threshold is reached
// during a highly-concurrent use case.
func TestMicroBatcher_MultiUser_Threshold(t *testing.T) {
	var batchProcessor = echo.NewEchoService[int](0)

	conf := DefaultConfig
	conf.Batch.Interval = 0
	conf.Batch.Threshold = 37

	microBatcher := NewMicroBatcher[int, int](conf, &batchProcessor, logger)
	microBatcher.Start()

	jobs := feeder.NewSequentialJobFeeder()

	var total atomic.Int32

	for j := 0; j < 100; j++ {
		for i := 0; i < 100; i++ {
			go func() {
				job := jobs.Feed()
				r := microBatcher.Submit(job)
				assert.Equal(t, job.Data, r.Ok)
				assert.Equal(t, job.Id, r.Id)
				assert.Nil(t, r.Err)
				total.Add(1)
			}()
		}
	}

	time.Sleep(5 * time.Second)
	// Note, we cannot use a wait group directly as the Submit job is synchronous
	// There will be some queued jobs because threshold has not been reached for the final batch
	// shutting down microBatcher will trigger final batch to be sent
	qLen := microBatcher.input.QueueLen()
	t.Log("Outstanding items in queue", "queue length", qLen)
	microBatcher.Shutdown()

	time.Sleep(5 * time.Second)
	assert.Equal(t, 10000, int(total.Load()))
}
