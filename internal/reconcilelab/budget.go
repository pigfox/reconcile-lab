package reconcilelab

import (
	"errors"
	"sync"
)

// ErrNoWorkerCapacity is returned when the global pool is fully committed and a
// new session could not be given even one reconcile worker.
//
// IT IS AN ERROR RATHER THAN A CLAMP, and the two are different on purpose. A
// clamp says "you asked for eight and got three", which is a smaller version of
// what was asked for and still a simulation. Zero workers is not a smaller
// simulation, it is no simulation — the competing writer would push every object
// off target and nothing would ever restore one — so the session is refused
// instead, and the refusal is delivered as a frame the page can read rather than
// as a hung connection.
var ErrNoWorkerCapacity = errors.New("reconcilelab: the global worker budget is fully committed")

// workerBudget is the process-wide ceiling on live reconcile goroutines.
//
// IT LIVES IN THIS PACKAGE, NOT IN THE HANDLER, for the same reason the
// per-session ceilings do: a limit the transport owns is a limit the second
// transport does not have. Sessions reserve here or they do not run.
type workerBudget struct {
	mu    sync.Mutex
	inUse int
}

var globalWorkerBudget = &workerBudget{}

// reserve commits up to want workers and returns how many were granted together
// with the function that gives them back. A grant of zero comes with a nil
// release and reports ErrNoWorkerCapacity — there is nothing to give back.
//
// A PARTIAL GRANT IS A CLAMP, not a failure: a session that asked for eight when
// three remain runs with three and says so, which is the same contract every
// other ceiling in this package keeps.
func (b *workerBudget) reserve(want int) (int, func(), error) {
	if want < 0 {
		want = 0
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	free := MaxGlobalWorkers - b.inUse
	if free <= 0 {
		return 0, nil, ErrNoWorkerCapacity
	}
	granted := want
	if granted > free {
		granted = free
	}
	if granted == 0 {
		return 0, nil, ErrNoWorkerCapacity
	}
	b.inUse += granted

	var once sync.Once
	release := func() {
		once.Do(func() {
			b.mu.Lock()
			defer b.mu.Unlock()
			b.inUse -= granted
		})
	}
	return granted, release, nil
}

func (b *workerBudget) count() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.inUse
}

// GlobalWorkersInUse is how many reconcile workers are committed across every
// live session. The page reports it so a visitor who gets a clamped pool can see
// that the reason is someone else's session rather than a defect in theirs.
func GlobalWorkersInUse() int { return globalWorkerBudget.count() }
