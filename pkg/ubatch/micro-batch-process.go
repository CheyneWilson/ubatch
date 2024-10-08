package ubatch

// uProcess and uProcessState are used to coordinate the state of different concurrent processes (goroutines)
//
// An example control structure is outlined below:
//
//	var uProc uProcess = newUProcess()
//	var log = ... /* a logger  */
//
//	func Start()  {
//	  startEventLoop()
//	}
//
//	func Stop()  {
//	  uProc.Stop()
//	}
//
//	func startEventLoop() {
//	  uProc.resetAndMarkStarting()
//	  go func() {
//	    for {
//	      select {
//	      case s := <-uProc.change:
//	        switch s {
//	        case STOPPED:
//	          log.Info("Event Loop Stopped")
//	          return
//	        case STARTING:
//	          log.Info("Event Loop Starting")
//	          uProc.markStarted()
//	        case STARTED:
//	          log.Info("Event Loop Started")
//	        case STOPPING:
//	          log.Info("Event Loop Stopping")
//	          uProc.markStopped()
//	        }
//	      }
//	    }
//	  }()
//	}
type uProcess struct {
	_       noCopy
	state   uProcessState
	stopped chan uEvent
	started chan uEvent
	change  chan uProcessState
}

type uProcessState int32

const (
	STOPPED  uProcessState = iota
	STARTING uProcessState = iota
	STARTED  uProcessState = iota
	STOPPING uProcessState = iota
)

type noCopy struct{}

func (*noCopy) Lock()   {}
func (*noCopy) Unlock() {}

func newUProcess() uProcess {
	return uProcess{
		state:   STOPPED,
		stopped: make(chan uEvent, 1),
		started: make(chan uEvent, 1),
		change:  make(chan uProcessState, 1),
	}
}

// Reset clear any previous stopped/started signals
func (up *uProcess) Reset() {
	// This protects against a double-stopped bug.
	// E.g. the actions Start, Stop, Stop, Start would cause the process eventLoop to Stop
	select {
	case <-up.stopped:
	default:
	}
	select {
	case <-up.started:
	default:
	}
}

func (up *uProcess) resetAndMarkStarting() {
	up.Reset()
	up.state = STARTING
	up.change <- STARTING
}

func (up *uProcess) markStarted() {
	up.state = STARTED
	up.change <- STARTED
	up.started <- uEvent{}
}

// Stop signals the stopped procedure should be run for this process
// It returns once the process has stopped
func (up *uProcess) Stop() {
	up.state = STOPPING
	up.change <- STOPPING
}

// StopIfStarted conditionally calls the Stop() method if this process is STARTED
func (up *uProcess) StopIfStarted() {
	if up.state == STARTED {
		up.Stop()
	}
}

// markStopped changes the state to STOPPED and triggers a stopped event
func (up *uProcess) markStopped() {
	up.state = STOPPED
	// We don't send a change event because the event loop has been stopped
	// e.g. don't do this ;p  up.change <- STOPPED
	up.stopped <- uEvent{}
}
