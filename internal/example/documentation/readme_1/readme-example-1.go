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
