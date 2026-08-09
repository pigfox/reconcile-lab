package reconcilelab

import (
	"errors"
	"testing"
)

// The budget is process-wide, so these tests take it and give it back rather
// than running in parallel with anything. A test that leaked a reservation would
// fail a later one in a way that looked unrelated — and with -shuffle=on, a
// different later one on every run.

func TestBudgetGrantsWhatWasAsked(t *testing.T) {
	b := &workerBudget{}

	granted, release, err := b.reserve(4)
	if err != nil {
		t.Fatalf("reserve(4): %v", err)
	}
	defer release()

	if granted != 4 {
		t.Errorf("granted %d, want 4", granted)
	}
	if b.count() != 4 {
		t.Errorf("count = %d, want 4", b.count())
	}
}

// A PARTIAL GRANT IS A CLAMP, NOT A FAILURE. A session that asked for eight when
// three remain runs with three and says so, which is the contract every other
// ceiling in this package keeps.
func TestBudgetGrantsWhatIsLeft(t *testing.T) {
	b := &workerBudget{}

	takenCount, releaseTaken, err := b.reserve(MaxGlobalWorkers - 3)
	if err != nil {
		t.Fatalf("first reserve: %v", err)
	}
	defer releaseTaken()
	if takenCount != MaxGlobalWorkers-3 {
		t.Fatalf("first reserve granted %d, want %d", takenCount, MaxGlobalWorkers-3)
	}

	granted, release, err := b.reserve(MaxWorkers)
	if err != nil {
		t.Fatalf("second reserve: %v", err)
	}
	defer release()
	if granted != 3 {
		t.Errorf("granted %d, want the 3 that were left", granted)
	}
}

// ZERO WORKERS IS NOT A SMALLER SIMULATION, IT IS NO SIMULATION — the competing
// writer would push every object off target and nothing would ever restore one.
// So a full house is refused rather than clamped to nothing.
func TestBudgetRefusesWhenFullyCommitted(t *testing.T) {
	b := &workerBudget{}

	_, release, err := b.reserve(MaxGlobalWorkers)
	if err != nil {
		t.Fatalf("reserve of the whole budget: %v", err)
	}
	defer release()

	granted, releaseNone, err := b.reserve(1)
	if !errors.Is(err, ErrNoWorkerCapacity) {
		t.Errorf("reserve against a full budget returned %v, want ErrNoWorkerCapacity", err)
	}
	if granted != 0 {
		t.Errorf("granted %d against a full budget, want 0", granted)
	}
	if releaseNone != nil {
		t.Error("a refused reservation came with a release function; there is nothing to give back")
	}
}

func TestBudgetRefusesARequestForNothing(t *testing.T) {
	b := &workerBudget{}

	granted, release, err := b.reserve(0)
	if !errors.Is(err, ErrNoWorkerCapacity) {
		t.Errorf("reserve(0) returned %v, want ErrNoWorkerCapacity", err)
	}
	if granted != 0 || release != nil {
		t.Errorf("reserve(0) granted %d with release=%v", granted, release != nil)
	}

	// A negative request is treated as zero rather than credited back into the
	// pool, which is what a naive subtraction would do.
	if _, _, err := b.reserve(-5); !errors.Is(err, ErrNoWorkerCapacity) {
		t.Errorf("reserve(-5) returned %v, want ErrNoWorkerCapacity", err)
	}
	if b.count() != 0 {
		t.Errorf("count = %d after refused reservations, want 0", b.count())
	}
}

func TestBudgetReleaseIsIdempotent(t *testing.T) {
	b := &workerBudget{}

	_, release, err := b.reserve(5)
	if err != nil {
		t.Fatalf("reserve: %v", err)
	}
	release()
	release()

	if b.count() != 0 {
		t.Errorf("count = %d after a double release, want 0 — the second release credited the pool twice", b.count())
	}
}

func TestGlobalWorkersInUseReadsTheRealCounter(t *testing.T) {
	before := GlobalWorkersInUse()

	granted, release, err := globalWorkerBudget.reserve(2)
	if err != nil {
		t.Fatalf("reserve: %v", err)
	}
	if got, want := GlobalWorkersInUse(), before+granted; got != want {
		t.Errorf("GlobalWorkersInUse = %d, want %d — the page reads a second copy of the counter", got, want)
	}
	release()
	if got := GlobalWorkersInUse(); got != before {
		t.Errorf("GlobalWorkersInUse = %d after release, want %d", got, before)
	}
}
