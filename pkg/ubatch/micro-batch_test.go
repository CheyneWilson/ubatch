package ubatch

import (
	"cheyne.nz/ubatch/internal/mock"
	. "cheyne.nz/ubatch/pkg/ubatch/types"
	"fmt"
	"os"
	"testing"
	"time"
)

// SimpleEndToEndTest checks that the Job sent to the MicroBatcher returns a Result
func TestSimpleEndToEnd(t *testing.T) {

	var batchProcessor BatchProcessor[string, string] = mock.NewEchoService[string, string](0)
	microBatcher := NewMicroBatcher[string, string](DefaultConfig, &batchProcessor)

	go func() {
		microBatcher.Run()
		j := Job[string]{
			Data: "Hello",
			Id:   1,
		}
		r := microBatcher.Submit(j)

		fmt.Fprintf(os.Stdout, "Hi hi is: %s\n", r.Ok)
	}()

	time.Sleep(5 * time.Second)
	microBatcher.Shutdown()
}

// TODO: test for duplicate IDs
