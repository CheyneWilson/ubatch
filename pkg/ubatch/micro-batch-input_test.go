package ubatch

import (
	"cheyne.nz/ubatch/pkg/ubatch/types"
	"cheyne.nz/ubatch/test/util/perf/feeder"
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
	"time"
)

// The InputReceiver uses a channel to support N concurrent users
// Jobs are transferred from the channel to the queue
// The waitForPending method is used for testing to ensure all submitted jobs have been queued
// While process should be very very fast. This wait ensures there are no flakey out-by-one errors
func (input *InputReceiver[T]) waitForPending() {
	for {
		if input.pending.Load() == 0 {
			break
		}
		time.Sleep(1 * time.Millisecond)
	}
}

// TestInputReceiver_SingleUserSubmit validates the job Submit process for a single consumer
func TestInputReceiver_SingleUserSubmit(t *testing.T) {
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

// TestInputReceiver_MultiUserSubmit validates the job Submit process for multiple concurrent consumers
func TestInputReceiver_MultiUserSubmit(t *testing.T) {
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
