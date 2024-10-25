package main

import (
	"cheyne.nz/ubatch"
	"cheyne.nz/ubatch/types"
	"fmt"
	"internal/mock/echo-batch-processor"
	"log/slog"
)

func main() {
	log := slog.Default()
	batchProcessor := echo.NewEchoService[string](0)
	microBatcher := ubatch.New(ubatch.DefaultConfig, &batchProcessor, log)
	microBatcher.Start()
	job := types.Job[string]{Data: "Hello", Id: 1}
	r := microBatcher.Submit(job)
	fmt.Printf("Got result: %+v\n", r)
	microBatcher.Shutdown()
}
