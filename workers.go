package workers

import (
	"context"
	"errors"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

var defaultWatchSignals = []os.Signal{syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL}

// Worker Contains the work function. Allows an input and output to a channel or another worker for pipeline work.
// Return nil if you want the Runner to continue otherwise any error will cause the Runner to shutdown and return the
// error.
type Worker interface {
	Work(ctx context.Context, in interface{}, out chan<- interface{}) error
}

type Runner struct {
	ctx     context.Context
	cancel  context.CancelFunc
	inChan  chan interface{}
	outChan chan interface{}
	limiter chan struct{}

	afterFunc  func(ctx context.Context, err error)
	workFunc   func(ctx context.Context, in interface{}, out chan<- interface{}) error
	beforeFunc func(ctx context.Context) error

	timeout time.Duration

	numWorkers int64
	started    *sync.Once
	wg         *sync.WaitGroup
}

// NewRunner Factory function for a new Runner.  The Runner will handle running the workers logic.
func NewRunner(ctx context.Context, w Worker, numWorkers int64) *Runner {
	runnerCtx, runnerCancel := signal.NotifyContext(ctx, defaultWatchSignals...)
	runner := &Runner{
		ctx:        runnerCtx,
		cancel:     runnerCancel,
		inChan:     make(chan interface{}, numWorkers),
		outChan:    nil,
		limiter:    make(chan struct{}, numWorkers),
		afterFunc:  func(ctx context.Context, err error) {},
		workFunc:   w.Work,
		beforeFunc: func(ctx context.Context) error { return nil },
		numWorkers: numWorkers,
		wg:         new(sync.WaitGroup),
		started:    new(sync.Once),
	}
	return runner
}

var ErrWorkerDone = errors.New("worker done")

// Send Send an object to the worker for processing.
func (r *Runner) Send(in interface{}) error {
	select {
	case <-r.ctx.Done():
		return ErrWorkerDone
	case r.inChan <- in:
	}
	return nil
}

// InFrom Set a worker to accept output from another worker(s).
func (r *Runner) InFrom(w ...*Runner) *Runner {
	for _, wr := range w {
		// create a chan for producer to close it without impacting other producers
		aggChan := make(chan interface{})
		go func() {
			for {
				select {
				case <-r.ctx.Done():
					// consumer is ending. Close chan to inform producer
					close(aggChan)
					return
				case msg, ok := <-aggChan:
					if !ok {
						return
					}
					r.inChan <- msg
				}
			}
		}()
		wr.SetOut(aggChan)
	}
	return r
}

// Start Starts the worker on processing.
func (r *Runner) Start() error {
	var err error
	r.started.Do(func() {
		err = r.beforeFunc(r.ctx)
		go r.work()
	})
	return err
}

// BeforeFunc Function to be run before worker starts processing.
func (r *Runner) BeforeFunc(f func(ctx context.Context) error) *Runner {
	r.beforeFunc = f
	return r
}

// AfterFunc Function to be run after worker has stopped.
func (r *Runner) AfterFunc(f func(ctx context.Context, err error)) *Runner {
	r.afterFunc = f
	return r
}

var ErrOutAlready = errors.New("out already set")

// SetOut Allows the setting of a workers out channel, if not already set.
func (r *Runner) SetOut(c chan interface{}) error {
	if r.outChan != nil {
		return ErrOutAlready
	}
	r.outChan = c
	return nil
}

// SetDeadline allows a time to be set when the workers should stop.
// Deadline needs to be handled by the IsDone method.
func (r *Runner) SetDeadline(t time.Time) *Runner {
	r.ctx, r.cancel = context.WithDeadline(r.ctx, t)
	return r
}

// SetTimeout allows a time duration to be set when the workers should stop.
// Timeout needs to be handled by the IsDone method.
func (r *Runner) SetTimeout(duration time.Duration) *Runner {
	r.timeout = duration
	return r
}

// Wait calls stop on workers and waits for the channel to drain.
// !!Should only be called when certain nothing will send to worker.
func (r *Runner) Wait() {
	r.wg.Wait()
}

// Stop Stops the processing of a worker and closes it's channel in.
// Returns a blocking channel with type error.
// !!Should only be called when certain nothing will send to worker.
func (r *Runner) Stop() *Runner {
	r.cancel()
	return r
}

type InputKey struct{}

// work Runs the before function and starts processing until one of three things happen.
// 1. A term signal is received or cancellation of context.
// 2. Stop function is called.
// 3. Worker returns an error.
func (r *Runner) work() {
	defer func() {
		// wait for workers to end
		r.wg.Wait()
		// indicate work is done
		r.cancel()
		// close outChan to indicate followers
		if r.outChan != nil {
			close(r.outChan)
		}
	}()

	for {
		select {
		case <-r.ctx.Done():
			return
		case input, ok := <-r.inChan:
			if !ok {
				return
			}
			r.limiter <- struct{}{}
			inputCtx, inputCancel := context.WithCancel(r.ctx)
			inputCtx = context.WithValue(inputCtx, InputKey{}, input)
			if r.timeout > 0 {
				inputCtx, inputCancel = context.WithTimeout(inputCtx, r.timeout)
			}
			r.wg.Add(1)
			go func() {
				defer func() {
					<-r.limiter
					r.wg.Done()
					inputCancel()
				}()
				r.afterFunc(inputCtx, r.workFunc(inputCtx, input, r.outChan))
			}()
		}
	}
}
