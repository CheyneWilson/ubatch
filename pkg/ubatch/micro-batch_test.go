package ubatch

import (
	"cheyne.nz/ubatch/internal/mock"
	. "cheyne.nz/ubatch/pkg/ubatch/types"
	"cheyne.nz/ubatch/test/util/perf/feeder"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

// TestMicroBatcher_EndToEnd_Single checks that the Job sent to the MicroBatcher returns a Result
func TestMicroBatcher_EndToEnd_Single(t *testing.T) {
	var batchProcessor BatchProcessor[string, string] = mock.NewEchoService[string, string](0)
	microBatcher := NewMicroBatcher[string, string](DefaultConfig, &batchProcessor, logger)
	microBatcher.Run()
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
	var batchProcessor BatchProcessor[int, int] = mock.NewEchoService[int, int](0)
	conf := DefaultConfig
	conf.Batch.Interval = 10 * time.Millisecond
	microBatcher := NewMicroBatcher[int, int](conf, &batchProcessor, logger)
	microBatcher.Run()
	jobs := feeder.NewSequentialJobFeeder()
	for i := 0; i < 10; i++ {
		r := microBatcher.Submit(jobs.Feed())
		assert.Equal(t, i, r.Ok)
		assert.Equal(t, Id(i), r.Id)
		assert.Nil(t, r.Err)
	}
	microBatcher.Shutdown()
}

// TODO: test for duplicate IDs, need to add this functionality in
