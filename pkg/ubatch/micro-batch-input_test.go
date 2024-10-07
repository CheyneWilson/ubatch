package ubatch

import (
	"cheyne.nz/ubatch/pkg/ubatch/types"
	"cheyne.nz/ubatch/test/util/perf/feeder"
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
	"time"
)

// The InputReceiver supports multiple users submitting Jobs concurrently.
// Jobs are transferred from the receiver channel to the queue.
// The waitForPending method is used for testing to ensure all submitted jobs arrived to the queue.
// While this process should be very, very fast, this wait ensures there are no flakey out-by-one errors.
func (input *InputReceiver[T]) waitForPending() {
	for {
		if input.pending.Load() == 0 {
			break
		}
		time.Sleep(1 * time.Millisecond)
	}
}

// TestInputReceiver_SingleUser_Submit validates the job Submit process for a single consumer
func TestInputReceiver_SingleUser_Submit(t *testing.T) {
	var conf = DefaultConfig.Input
	var inputReceiver = newInputReceiver[int](conf)
	jobs := feeder.NewSequentialJobFeeder()
	inputReceiver.Start()

	for i := 0; i < 100; i++ {
		inputReceiver.Submit(jobs.Feed())
	}
	inputReceiver.waitForPending()
	assert.Equal(t, 100, len(*inputReceiver.queue))

	for i := 0; i < 100; i++ {
		inputReceiver.Submit(jobs.Feed())
	}
	inputReceiver.waitForPending()
	assert.Equal(t, 200, len(*inputReceiver.queue))

	inputReceiver.Start()
	for i := 0; i < 1000; i++ {
		inputReceiver.Submit(jobs.Feed())
	}
	inputReceiver.waitForPending()
	assert.Equal(t, 1200, len(*inputReceiver.queue))

	inputReceiver.Start()
	for i := 0; i < 10000; i++ {
		inputReceiver.Submit(jobs.Feed())
	}
	inputReceiver.waitForPending()
	assert.Equal(t, 11200, len(*inputReceiver.queue))

	// The queue should contain sequential jobs from 1 to 11200
	for i := 0; i < 11200; i++ {
		assert.Equal(t, (*inputReceiver.queue)[i].Data, i+1)
		assert.Equal(t, (*inputReceiver.queue)[i].Id, i+1)
	}
}

// TestInputReceiver_MultiUser_Submit validates the job Submit process for multiple concurrent consumers
func TestInputReceiver_MultiUser_Submit(t *testing.T) {
	var conf = DefaultConfig.Input
	var inputReceiver = newInputReceiver[int](conf)
	jobs := feeder.NewSequentialJobFeeder()
	inputReceiver.Start()

	var wg sync.WaitGroup
	for n := 0; n < 100; n++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 1000; i++ {
				inputReceiver.Submit(jobs.Feed())
			}
		}()
	}
	wg.Wait()
	inputReceiver.waitForPending()

	assert.Equal(t, 100000, len(*inputReceiver.queue))

	set := make(map[types.Id]bool)
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
	var conf = DefaultConfig.Input
	var inputReceiver = newInputReceiver[int](conf)
	jobs := feeder.NewSequentialJobFeeder()
	inputReceiver.Start()

	for i := 0; i < 100; i++ {
		inputReceiver.Submit(jobs.Feed())
	}
	inputReceiver.waitForPending()
	jobsBatch1 := inputReceiver.PrepareBatch()
	assert.Equal(t, len(*inputReceiver.queue), 0)
	assert.Equal(t, len(jobsBatch1), 100)

	for i := 0; i < 100; i++ {
		assert.Equal(t, jobsBatch1[i].Data, i+1)
		assert.Equal(t, jobsBatch1[i].Id, types.Id(i+1))
	}

	for i := 0; i < 100; i++ {
		inputReceiver.Submit(jobs.Feed())
	}
	inputReceiver.waitForPending()
	jobsBatch2 := inputReceiver.PrepareBatch()
	assert.Equal(t, len(*inputReceiver.queue), 0)
	assert.Equal(t, len(jobsBatch2), 100)

	const idOffset = 101
	for i := 0; i < 100; i++ {
		assert.Equal(t, jobsBatch2[i].Data, i+idOffset)
		assert.Equal(t, jobsBatch2[i].Id, types.Id(i+idOffset))
	}
}

// TestInputReceiver_SingleUser_Concurrent_PrepareBatch tests the Submit and PrepareBatch methods being called
// concurrently. It is semi-deterministic test, all batched jobs will appear in order, but the size of each
// batch could vary depending on the contents of the queue when PrepareBatch triggers
func TestInputReceiver_SingleUser_Concurrent_PrepareBatch(t *testing.T) {
	var conf = DefaultConfig.Input
	var inputReceiver = newInputReceiver[int](conf)
	jobs := feeder.NewSequentialJobFeeder()
	inputReceiver.Start()

	var batches = make([][]types.Job[int], 0, 10)

	for i := 0; i < 100; i++ {
		inputReceiver.Submit(jobs.Feed())
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
			i += 1
			// fmt.Fprintf(os.Stdout, "Job Id is %d\n", job.Id)
			if job.Id != types.Id(i) {
				t.Fatalf("Missing Id '%d' from sequence", i)
			}
		}
	}

	assert.Equal(t, expectedBatchCount, len(batches))
}

// TestInputReceiver_MultiUser_Concurrent_PrepareBatch verifies that
func TestInputReceiver_MultiUser_Concurrent_PrepareBatch(t *testing.T) {
	var conf = DefaultConfig.Input
	var inputReceiver = newInputReceiver[int](conf)
	jobs := feeder.NewSequentialJobFeeder()
	inputReceiver.Start()

	var batches = make([][]types.Job[int], 0)

	var wg sync.WaitGroup
	for n := 0; n < 100; n++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 1000; i++ {
				inputReceiver.Submit(jobs.Feed())
				// Prepare a batch every 100 iterations, due to the concurrency of 100 goroutines, this will result
				// in some janky batch sizes, both small and large
				if i%100 == 0 {
					b := inputReceiver.PrepareBatch()
					batches = append(batches, b)
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
	assert.Equal(t, 0, inputReceiver.pending.Load())

	// There should be 100,000 jobs with unique IDs
	set := make(map[types.Id]bool)
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
	// FIXME: address output queue bug this was ~approx 99979
	assert.Equal(t, totalJobs, 100000)
}
