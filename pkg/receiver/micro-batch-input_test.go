package receiver

import (
	. "cheyne.nz/ubatch/common/types"
	"github.com/stretchr/testify/assert"
	"internal/feeder"
	"log/slog"
	"os"
	"sync"
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

// waitForPending returns when where are no more items in pending in the receiver channel.
// Calling Stop, waitForPending, and PrepareBatch sequentially will ensure all inputted items are returned.
func (input *InputReceiver[T]) waitForPending() {
	for {
		input.muPending.RLock()
		p := input.pending
		input.muPending.RUnlock()
		if p == 0 {
			break
		}
		time.Sleep(1 * time.Millisecond)
	}
}

// TestInputReceiver_SingleUser_Submit tests the job Submit method and receive process for a single consumer
func TestInputReceiver_SingleUser_Submit(t *testing.T) {
	var conf = DefaultConfig
	var inputReceiver = New[Job[int]](conf, logger, nil)
	jobs := feeder.NewSequentialJobFeeder()
	inputReceiver.Start()

	for i := 0; i < 100; i++ {
		_ = inputReceiver.Submit(jobs.Feed())
	}
	inputReceiver.flushPending()
	assert.Equal(t, 100, inputReceiver.QueueLen())

	for i := 0; i < 100; i++ {
		_ = inputReceiver.Submit(jobs.Feed())
	}
	inputReceiver.flushPending()
	assert.Equal(t, 200, inputReceiver.QueueLen())

	inputReceiver.Start()
	for i := 0; i < 1000; i++ {
		_ = inputReceiver.Submit(jobs.Feed())
	}
	inputReceiver.flushPending()
	assert.Equal(t, 1200, inputReceiver.QueueLen())

	inputReceiver.Start()
	for i := 0; i < 10000; i++ {
		_ = inputReceiver.Submit(jobs.Feed())
	}
	inputReceiver.flushPending()

	assert.Equal(t, 11200, inputReceiver.QueueLen())
	// The queue should contain sequential jobs from 1 to 11200
	for i := 0; i < 11200; i++ {
		expected := i
		assert.Equal(t, (*inputReceiver.queue)[i].Data, expected)
		assert.Equal(t, (*inputReceiver.queue)[i].Id, Id(expected))
	}
}

// TestInputReceiver_MultiUser_Submit tests the job Submit method and receive process for multiple concurrent consumers
func TestInputReceiver_MultiUser_Submit(t *testing.T) {
	var conf = DefaultConfig
	var inputReceiver = New[Job[int]](conf, logger, nil)
	jobs := feeder.NewSequentialJobFeeder()
	inputReceiver.Start()

	var wg sync.WaitGroup
	for n := 0; n < 100; n++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 1000; i++ {
				_ = inputReceiver.Submit(jobs.Feed())
			}
		}()
	}
	wg.Wait()
	inputReceiver.waitForPending()

	set := make(map[Id]bool)

	assert.Equal(t, 100000, inputReceiver.QueueLen())

	// There should be 100,000 jobs with unique IDs
	for i := 0; i < len(*inputReceiver.queue); i++ {
		id := (*inputReceiver.queue)[i].Id
		//fmt.Fprintf(os.Stdout, "id is %d\n", id)
		if set[id] != true {
			set[id] = true
		} else {
			t.Fatalf("Duplicate Job/Result Id '%d'", id)
		}
	}
}

// TestInputReceiver_SingleUser_PrepareBatch
func TestInputReceiver_SingleUser_PrepareBatch(t *testing.T) {
	var conf = DefaultConfig
	var inputReceiver = New[Job[int]](conf, logger, nil)
	jobs := feeder.NewSequentialJobFeeder()
	inputReceiver.Start()

	for i := 0; i < 100; i++ {
		_ = inputReceiver.Submit(jobs.Feed())
	}
	inputReceiver.waitForPending()
	jobsBatch1 := inputReceiver.PrepareBatch()
	assert.Equal(t, len(*inputReceiver.queue), 0)
	assert.Equal(t, len(jobsBatch1), 100)

	for i := 0; i < 100; i++ {
		assert.Equal(t, jobsBatch1[i].Data, i)
		assert.Equal(t, jobsBatch1[i].Id, Id(i))
	}

	for i := 0; i < 100; i++ {
		_ = inputReceiver.Submit(jobs.Feed())
	}
	inputReceiver.waitForPending()
	jobsBatch2 := inputReceiver.PrepareBatch()
	assert.Equal(t, inputReceiver.QueueLen(), 0)
	assert.Equal(t, len(jobsBatch2), 100)

	const idOffset = 100
	for i := 0; i < 100; i++ {
		assert.Equal(t, jobsBatch2[i].Data, i+idOffset)
		assert.Equal(t, jobsBatch2[i].Id, Id(i+idOffset))
	}
}

// TestInputReceiver_SingleUser_Concurrent_PrepareBatch tests the Submit and PrepareBatch methods being called
// concurrently. It is semi-deterministic test, all batched jobs will appear in order, but the size of each
// batch could vary depending on the contents of the queue when PrepareBatch triggers
func TestInputReceiver_SingleUser_Concurrent_PrepareBatch(t *testing.T) {
	var conf = DefaultConfig
	var inputReceiver = New[Job[int]](conf, logger, nil)
	jobs := feeder.NewSequentialJobFeeder()
	inputReceiver.Start()

	var batches = make([][]Job[int], 0, 10)

	for i := 0; i < 100; i++ {
		_ = inputReceiver.Submit(jobs.Feed())
		if i%10 == 0 {
			b := inputReceiver.PrepareBatch()
			batches = append(batches, b)
		}
	}
	inputReceiver.waitForPending()
	expectedBatchCount := 10

	// In rare cases we could have a final batch of 1 item if the inputReceiver hasn't yet processed the
	// last of the pending items (it's only 1 because Submit is a synchronous call)
	if b := inputReceiver.PrepareBatch(); len(b) > 0 {
		batches = append(batches, b)
		expectedBatchCount += 1
	}

	// There should be 100 jobs in total, and due to the deterministic nature of this test all the Ids
	// should be in sequential order
	i := 0
	for _, batch := range batches {
		for _, job := range batch {
			if job.Id != Id(i) {
				t.Fatalf("Missing Id '%d' from sequence", i)
			}
			i += 1
		}
	}

	assert.Equal(t, expectedBatchCount, len(batches))
}

// TestInputReceiver_MultiUser_Concurrent_PrepareBatch verifies that
func TestInputReceiver_MultiUser_Concurrent_PrepareBatch(t *testing.T) {
	var conf = DefaultConfig
	var inputReceiver = New[Job[int]](conf, logger, nil)
	jobs := feeder.NewSequentialJobFeeder()
	inputReceiver.Start()

	var batches = make([][]Job[int], 0)
	var batchMu = sync.Mutex{}

	var wg sync.WaitGroup
	for n := 0; n < 100; n++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 1000; i++ {
				_ = inputReceiver.Submit(jobs.Feed())
				// Prepare a batch every 100 iterations, due to the concurrency of 100 goroutines, this will result
				// in some janky batch sizes, both small and large
				if i%100 == 0 {
					b := inputReceiver.PrepareBatch()
					batchMu.Lock()
					batches = append(batches, b)
					batchMu.Unlock()
				}
			}
		}()
	}
	wg.Wait()
	inputReceiver.waitForPending()

	// In rare cases we could have a final batch of 1 item if the inputReceiver hasn't yet processed the
	// last of the pending items (it's only 1 because Submit is a synchronous call)
	if b := inputReceiver.PrepareBatch(); len(b) > 0 {
		batches = append(batches, b)
	}

	assert.Equal(t, 0, len(*inputReceiver.queue))
	assert.Equal(t, 0, inputReceiver.pending)

	// There should be 100,000 jobs with unique IDs
	set := make(map[Id]bool)
	totalJobs := 0
	for _, batch := range batches {
		for _, job := range batch {
			totalJobs += 1
			if set[job.Id] != true {
				set[job.Id] = true
			} else {
				t.Fatalf("Duplicate Job/Result Id '%d'", job.Id)
			}

		}
	}
	assert.Equal(t, totalJobs, 100000)
}

func TestInputReceiver_nilLogger(t *testing.T) {
	var conf = DefaultConfig
	var inputReceiver = New[Job[int]](conf, nil, nil)
	jobs := feeder.NewSequentialJobFeeder()
	inputReceiver.Start()
	_ = inputReceiver.Submit(jobs.Feed())
	inputReceiver.waitForPending()
	jobsBatch := inputReceiver.PrepareBatch()
	assert.Equal(t, jobsBatch[0].Id, Id(0))
}

// TestInputReceiver_StopStart provides basic validation for the Stop/Start methods of the InputReceiver
// It also tests the ErrJobRefused behaviour
func TestInputReceiver_StopStart(t *testing.T) {
	var conf = DefaultConfig
	var inputReceiver = New[Job[int]](conf, logger, nil)
	jobs := feeder.NewSequentialJobFeeder()

	// If the inputReceiver has not been started, it should refuse jobs
	err := inputReceiver.Submit(jobs.Feed())
	assert.Equal(t, ErrJobRefused, err)

	inputReceiver.Start()
	assert.Equal(t, STARTED, inputReceiver.control.state)

	// Once started, jobs should be accepted without error
	err = inputReceiver.Submit(jobs.Feed())
	if err != nil {
		t.Fatal("did not expect an error", err)
	}

	inputReceiver.Stop()
	assert.Equal(t, STOPPED, inputReceiver.control.state)

	// And when stopped it should error again
	err = inputReceiver.Submit(jobs.Feed())
	assert.Equal(t, ErrJobRefused, err)

	// Start everything again to check the state transition
	inputReceiver.Start()
	assert.Equal(t, STARTED, inputReceiver.control.state)

	// It should accept jobs once more
	err = inputReceiver.Submit(jobs.Feed())
	if err != nil {
		t.Fatal("did not expect an error", err)
	}

	// And finally stopped to check the last state transition
	inputReceiver.Stop()
	assert.Equal(t, STOPPED, inputReceiver.control.state)

	// And when stopped it should once more
	err = inputReceiver.Submit(jobs.Feed())
	assert.Equal(t, ErrJobRefused, err)
}

// TODO: include basic test for the onEnqueue
