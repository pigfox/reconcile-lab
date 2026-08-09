package reconcilelab

import (
	"sync"
	"time"
)

// Clock is the simulation's ONLY source of time and of delay. Nothing in this
// package calls time.Now or time.Sleep directly, and that is what makes the
// tests both deterministic and instant: the same simulation that paces itself
// against the wall clock in production runs to completion in microseconds under
// VirtualClock, producing the same numbers.
//
// PROVENANCE, stated because this is the second copy of a subtle concurrency
// primitive in this repository. The contract and the advance rule are derived
// from internal/backpressurelab/clock.go, read at PF-S193c rather than recalled.
// It is a re-implementation and not a vendored copy, because that file declares
// `package backpressurelab` and a byte-identical copy would not compile here.
// THE RIGHT FIX IS A SHARED PACKAGE — lifting Clock into internal/simclock and
// having both demos consume it — and that is deliberately NOT done here: it
// would edit a shipped demo's package under a directive scoped to a new one.
// Recorded in NOTES-PF-S193.md as follow-up rather than left as an unremarked
// second copy of a primitive.
//
// IT IS DELIBERATELY MORE THAN Now+Sleep. The two extra operations are what a
// virtual clock needs in order to know when it is safe to move time forward:
//
//   - Join/Leave say how many goroutines are participating. A clock that does
//     not know cannot tell "everyone is waiting" from "one goroutine has not
//     started yet".
//   - Blocked says how many of them are waiting on something that is NOT a
//     sleep — a reconcile worker on an empty requeue set. Without it a virtual
//     clock would advance past a wake that was about to happen.
//
// The real clock implements both as no-ops, so the cost of carrying them is two
// empty methods on the production path.
type Clock interface {
	// Now is the current simulation instant.
	Now() time.Time

	// Join registers a participating goroutine. Callers Join BEFORE starting the
	// goroutine, so registration order is the order of the `go` statements and
	// not whatever order the scheduler happens to run them in — VirtualClock
	// breaks deadline ties on that order, so it is what makes a tie deterministic.
	Join(name string) Participant

	// Blocked adjusts the count of participants waiting on something other than
	// time. It is called with a POSITIVE delta by the goroutine about to wait and
	// with a NEGATIVE delta BY THE GOROUTINE DOING THE WAKING, before it signals
	// — never by the waiter after it wakes. See Lab.park for why that asymmetry
	// is load-bearing.
	Blocked(delta int)

	// Interrupt ends every outstanding and future Sleep, which is how a run
	// unwinds. It is idempotent.
	Interrupt()
}

// Participant is one goroutine's handle on the clock.
type Participant interface {
	// Sleep blocks for d. It reports false if the run was interrupted, in which
	// case the caller must unwind rather than continue.
	Sleep(d time.Duration) bool

	// Leave deregisters the goroutine. It is idempotent, so `defer p.Leave()` is
	// safe alongside an explicit call.
	Leave()
}

// --- the real clock ---------------------------------------------------------

// realClock paces the simulation against wall time. This is what a live session
// runs on, so a visitor watching the stream sees conflicts accumulate at the
// rate the contention actually produces them.
type realClock struct {
	once sync.Once
	stop chan struct{}
}

// NewRealClock returns the wall-clock implementation.
func NewRealClock() Clock { return &realClock{stop: make(chan struct{})} }

func (c *realClock) Now() time.Time { return time.Now() }

func (c *realClock) Join(string) Participant { return realParticipant{c: c} }

// Blocked is a no-op: wall time advances whether or not anyone is waiting on it.
func (c *realClock) Blocked(int) {}

func (c *realClock) Interrupt() { c.once.Do(func() { close(c.stop) }) }

type realParticipant struct{ c *realClock }

func (p realParticipant) Sleep(d time.Duration) bool {
	if d <= 0 {
		select {
		case <-p.c.stop:
			return false
		default:
			return true
		}
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-p.c.stop:
		return false
	}
}

func (p realParticipant) Leave() {}

// --- the virtual clock ------------------------------------------------------

// VirtualClock runs the simulation in virtual time: sleeps return as soon as
// every other participant is also waiting, at which point the clock jumps
// straight to the earliest deadline. A ninety-second session finishes in
// microseconds and produces the timestamps it would have produced live.
//
// HOW IT STAYS DETERMINISTIC. Time moves only when every registered participant
// is waiting — sleeping or blocked — and each advance releases EXACTLY ONE
// sleeper, the one with the earliest deadline, ties broken by join order. So at
// most one goroutine is runnable at a time, and the sequence of instants a run
// visits does not depend on the Go scheduler.
//
// The one place two goroutines are briefly runnable together is a wake: a writer
// that enqueues a key releases a parked reconcile worker and then carries on.
// That window cannot advance time — the waker decrements the blocked count
// BEFORE it signals, so the clock sees fewer waiters than participants for the
// whole of it — so the two may interleave but neither can be handed a timestamp
// the other did not see coming.
//
// WHY EVERY STORE CALL IN THIS PACKAGE COSTS A SLEEP, since it is the question a
// reader of Lab.reconcile will have. A retry policy that retried with no delay
// at all would be a goroutine that is never sleeping and never blocked, so this
// clock could never reach quiescence and virtual time would stop. Charging each
// read and each write one simulated round trip is what keeps the clock moving —
// and it is also the honest model, because an immediate retry against a real API
// server costs a round trip too. See apiRoundTrip in constants.go.
type VirtualClock struct {
	mu   sync.Mutex
	cond *sync.Cond

	now         time.Time
	interrupted bool

	live    int
	blocked int
	// pending counts sleepers that have been released but have not yet re-taken
	// the lock. Time must not advance again until they have, or a single
	// quiescent moment would be advanced through twice.
	pending int

	parts []*vparticipant
}

type vparticipant struct {
	c        *VirtualClock
	name     string
	sleeping bool
	ready    bool
	left     bool
	deadline time.Time
}

// NewVirtualClock starts a virtual clock at the given instant. The start instant
// is a parameter rather than time.Now() so a test's timestamps are reproducible
// from the source alone.
func NewVirtualClock(start time.Time) *VirtualClock {
	c := &VirtualClock{now: start}
	c.cond = sync.NewCond(&c.mu)
	return c
}

func (c *VirtualClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *VirtualClock) Join(name string) Participant {
	c.mu.Lock()
	defer c.mu.Unlock()
	p := &vparticipant{c: c, name: name}
	c.parts = append(c.parts, p)
	c.live++
	return p
}

func (c *VirtualClock) Blocked(delta int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.blocked += delta
	if delta > 0 {
		c.advanceLocked()
	}
}

func (c *VirtualClock) Interrupt() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.interrupted {
		c.interrupted = true
		c.cond.Broadcast()
	}
}

func (p *vparticipant) Sleep(d time.Duration) bool {
	c := p.c
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.interrupted {
		return false
	}
	if d <= 0 {
		return true
	}

	p.sleeping = true
	p.ready = false
	p.deadline = c.now.Add(d)
	c.blocked++
	c.advanceLocked()

	for !p.ready && !c.interrupted {
		c.cond.Wait()
	}

	p.sleeping = false
	c.blocked--
	if p.ready {
		p.ready = false
		c.pending--
	}
	return !c.interrupted
}

func (p *vparticipant) Leave() {
	c := p.c
	c.mu.Lock()
	defer c.mu.Unlock()
	if p.left {
		return
	}
	p.left = true
	c.live--
	c.advanceLocked()
}

// advanceLocked moves time to the next deadline if — and only if — every
// participant is waiting and no previously-released sleeper is still in flight.
// It releases exactly one sleeper.
//
// It returns without doing anything when every participant is blocked on
// something other than time and nothing is sleeping. That state is a genuine
// deadlock in the simulation, and advancing time would not resolve it; the run
// unwinds through Interrupt instead.
func (c *VirtualClock) advanceLocked() {
	if c.interrupted || c.pending > 0 || c.live == 0 || c.blocked < c.live {
		return
	}

	var next *vparticipant
	for _, p := range c.parts {
		if !p.sleeping || p.ready || p.left {
			continue
		}
		if next == nil || p.deadline.Before(next.deadline) {
			next = p
		}
	}
	if next == nil {
		return
	}

	if next.deadline.After(c.now) {
		c.now = next.deadline
	}
	next.ready = true
	c.pending++
	c.cond.Broadcast()
}
