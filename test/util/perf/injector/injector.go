package injector

// The injector package is used for supplying different load profiles (usage) to micro-batcher tests.
// The intent is to verify its behaviour is correct under various concurrency scenarios.
//
// TODO: the Simulation/Scenario/Injector concepts from Gatling have been blurred together for expediency so time
//   could be instead spent on testing / verifying the MicroBatcher. This can be fleshed out as needed.

import (
	"time"
)

type event struct{}

type Scenario struct {
	Run func()
}

// AtOnceUsers runs `n` copies of a Scenario simultaneously
func (scn Scenario) AtOnceUsers(n int) {
	for i := 0; i < n; i++ {
		go func() {
			scn.Run()
		}()
	}
}

// Once runs a Scenario once
func (scn *Scenario) Once() {
	go func() {
		scn.Run()
	}()
}

// ConstUsersPerSecond starts `n` copies of a Scenario every second for a set duration
// each started Scenario runs until completion
func (scn Scenario) ConstUsersPerSecond(nUsers int, duration time.Duration) {
	timer := time.NewTimer(1 * time.Second)
	go func() {
		time.Sleep(duration)
		timer.Stop()
	}()

	for {
		select {
		case <-timer.C:
			for i := 0; i < nUsers; i++ {
				go func() {
					scn.Run()
				}()
			}
		}
	}
}
