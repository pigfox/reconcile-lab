package reconcilelab

import (
	"testing"
	"time"
)

// epoch is a fixed start instant. It is a literal rather than time.Now() so a
// virtual run's timestamps are reproducible from the source alone.
var epoch = time.Date(2026, time.August, 8, 12, 0, 0, 0, time.UTC)

// --- the real clock ----------------------------------------------------------

// THE ONE WALL-CLOCK WAIT IN THIS PACKAGE'S TESTS, and it is here rather than
// anywhere else on purpose. Every test of the SIMULATION runs on VirtualClock
// and finishes in microseconds; realClock, though, is the implementation whose
// whole job is to wait on a real timer, and the branch that returns true when
// the timer fires cannot be reached without one firing. A nanosecond is the
// smallest thing that reaches it, and it is not a duration anyone waits for.
func TestRealClockTimerBranchFires(t *testing.T) {
	c := NewRealClock()
	p := c.Join("solo")
	defer p.Leave()

	if !p.Sleep(time.Nanosecond) {
		t.Error("a real-clock sleep that was never interrupted reported an interruption")
	}
}

func TestRealClockReadsWallTime(t *testing.T) {
	c := NewRealClock()

	before := time.Now()
	got := c.Now()
	after := time.Now()

	if got.Before(before) || got.After(after) {
		t.Errorf("Now() = %v, outside the wall-clock interval [%v, %v] it was read in", got, before, after)
	}
}

func TestRealClockZeroSleepDoesNotWait(t *testing.T) {
	c := NewRealClock()
	p := c.Join("solo")

	if !p.Sleep(0) {
		t.Error("Sleep(0) reported an interruption before Interrupt was called")
	}

	// Blocked is a no-op on the real clock; calling it must not panic or change
	// anything a later sleep can observe.
	c.Blocked(1)
	c.Blocked(-1)
	if !p.Sleep(0) {
		t.Error("Blocked changed the real clock's behavior; it is meant to be a no-op")
	}

	p.Leave()
	p.Leave() // idempotent
}

func TestRealClockInterruptEndsEverySleep(t *testing.T) {
	c := NewRealClock()
	p := c.Join("solo")
	defer p.Leave()

	c.Interrupt()
	c.Interrupt() // idempotent: a second call must not close a closed channel

	if p.Sleep(0) {
		t.Error("Sleep(0) returned true after Interrupt")
	}
	// An hour-long sleep must return immediately, which is the whole point of
	// Interrupt: a session unwinds without waiting out its own ceilings.
	if p.Sleep(time.Hour) {
		t.Error("an hour-long sleep returned true after Interrupt")
	}
}

// --- the virtual clock -------------------------------------------------------

func TestVirtualClockStartsWhereItWasTold(t *testing.T) {
	c := NewVirtualClock(epoch)
	if got := c.Now(); !got.Equal(epoch) {
		t.Errorf("Now() = %v, want the start instant %v", got, epoch)
	}
}

func TestVirtualClockAdvancesToTheDeadline(t *testing.T) {
	c := NewVirtualClock(epoch)
	p := c.Join("solo")

	if !p.Sleep(7 * time.Second) {
		t.Fatal("the sleep reported an interruption")
	}
	if got, want := c.Now(), epoch.Add(7*time.Second); !got.Equal(want) {
		t.Errorf("Now() = %v, want %v — a lone participant's sleep must move time to its deadline", got, want)
	}
	p.Leave()
}

func TestVirtualClockZeroSleepReturnsWithoutMovingTime(t *testing.T) {
	c := NewVirtualClock(epoch)
	p := c.Join("solo")
	defer p.Leave()

	if !p.Sleep(0) {
		t.Error("Sleep(0) reported an interruption")
	}
	if got := c.Now(); !got.Equal(epoch) {
		t.Errorf("Now() = %v after Sleep(0); time must not move for a zero sleep", got)
	}
}

// THE EARLIEST DEADLINE WAKES FIRST, AND ONLY IT. This is the property that
// makes a virtual run deterministic: at most one goroutine is runnable at a
// time, so the order a run visits its instants in does not depend on the Go
// scheduler.
func TestVirtualClockReleasesTheEarliestSleeperFirst(t *testing.T) {
	c := NewVirtualClock(epoch)
	slow := c.Join("slow")
	fast := c.Join("fast")

	order := make(chan string, 2)
	go func() {
		slow.Sleep(30 * time.Second)
		order <- "slow"
		slow.Leave()
	}()
	go func() {
		fast.Sleep(5 * time.Second)
		order <- "fast"
		fast.Leave()
	}()

	if got := <-order; got != "fast" {
		t.Errorf("first wake was %q, want the earlier deadline %q", got, "fast")
	}
	if got := <-order; got != "slow" {
		t.Errorf("second wake was %q, want %q", got, "slow")
	}
	if got, want := c.Now(), epoch.Add(30*time.Second); !got.Equal(want) {
		t.Errorf("Now() = %v, want %v", got, want)
	}
}

// A TIE IS BROKEN ON JOIN ORDER, which is why callers Join from the goroutine
// that spawns them rather than from the goroutine itself.
func TestVirtualClockBreaksTiesOnJoinOrder(t *testing.T) {
	c := NewVirtualClock(epoch)
	first := c.Join("first")
	second := c.Join("second")

	order := make(chan string, 2)
	go func() {
		second.Sleep(5 * time.Second)
		order <- "second"
		second.Leave()
	}()
	go func() {
		first.Sleep(5 * time.Second)
		order <- "first"
		first.Leave()
	}()

	if got := <-order; got != "first" {
		t.Errorf("the tie went to %q; it must go to the participant that joined first", got)
	}
	<-order
}

// TIME DOES NOT MOVE WHILE A PARTICIPANT IS AWAKE. The blocked count is what
// tells the clock the difference between "everyone is waiting" and "one of them
// has work to do", and without it the clock would advance past an event that was
// about to happen.
func TestVirtualClockWaitsForEveryParticipant(t *testing.T) {
	c := NewVirtualClock(epoch)
	sleeper := c.Join("sleeper")
	awake := c.Join("awake")

	woke := make(chan struct{})
	go func() {
		sleeper.Sleep(time.Second)
		close(woke)
		sleeper.Leave()
	}()

	// `awake` has neither slept nor declared itself blocked, so the clock has one
	// participant it is still waiting for and must not have moved.
	select {
	case <-woke:
		t.Fatal("the sleeper woke while another participant was still runnable")
	default:
	}
	if got := c.Now(); !got.Equal(epoch) {
		t.Errorf("Now() = %v; time moved while a participant was awake", got)
	}

	// Declaring it blocked completes the quiescent set and releases the sleeper.
	c.Blocked(1)
	<-woke
	c.Blocked(-1)
	awake.Leave()
}

// A NEGATIVE DELTA MUST NOT ADVANCE TIME. Blocked is called with a negative
// delta by a waker, which is by definition a goroutine that is about to run —
// advancing there is the exact bug the asymmetry in Lab.park exists to avoid.
func TestVirtualClockNegativeBlockedDoesNotAdvance(t *testing.T) {
	c := NewVirtualClock(epoch)
	p := c.Join("solo")
	defer p.Leave()

	c.Blocked(-1)
	if got := c.Now(); !got.Equal(epoch) {
		t.Errorf("Now() = %v; a negative Blocked delta moved time", got)
	}
	c.Blocked(1)
}

func TestVirtualClockInterruptEndsSleeps(t *testing.T) {
	c := NewVirtualClock(epoch)
	p := c.Join("solo")

	c.Interrupt()
	c.Interrupt() // idempotent

	if p.Sleep(time.Hour) {
		t.Error("a sleep returned true after Interrupt")
	}
	if p.Sleep(0) {
		t.Error("Sleep(0) returned true after Interrupt")
	}
	p.Leave()
}

// A SLEEP ALREADY IN FLIGHT IS ENDED BY Interrupt, not merely a later one. This
// is what lets Run unwind: the goroutines are parked on their own deadlines when
// the session is closed.
func TestVirtualClockInterruptWakesAnInFlightSleep(t *testing.T) {
	c := NewVirtualClock(epoch)
	sleeper := c.Join("sleeper")
	holder := c.Join("holder")

	result := make(chan bool, 1)
	go func() {
		result <- sleeper.Sleep(time.Hour)
		sleeper.Leave()
	}()

	// holder never waits, so the clock cannot reach quiescence and the sleep
	// above cannot end on its deadline. Only Interrupt can end it.
	c.Interrupt()
	if <-result {
		t.Error("an in-flight sleep reported success after Interrupt")
	}
	holder.Leave()
}

func TestVirtualClockLeaveIsIdempotentAndFreesTheSet(t *testing.T) {
	c := NewVirtualClock(epoch)
	waiting := c.Join("waiting")
	leaving := c.Join("leaving")

	woke := make(chan struct{})
	go func() {
		waiting.Sleep(time.Second)
		close(woke)
		waiting.Leave()
	}()

	// Two participants, one of them awake, so nothing can advance. The awake one
	// leaving reduces the set to the sleeper, which then advances.
	leaving.Leave()
	<-woke

	leaving.Leave() // idempotent: a second Leave must not decrement again
	if got, want := c.Now(), epoch.Add(time.Second); !got.Equal(want) {
		t.Errorf("Now() = %v, want %v", got, want)
	}
}

// AN EMPTY PARTICIPANT SET ADVANCES NOTHING. advanceLocked is reached from
// Leave, so the last participant leaving must not try to move time on an empty
// set.
func TestVirtualClockEmptySetDoesNotAdvance(t *testing.T) {
	c := NewVirtualClock(epoch)
	p := c.Join("solo")
	p.Leave()

	if got := c.Now(); !got.Equal(epoch) {
		t.Errorf("Now() = %v; time moved after the last participant left", got)
	}
}

// EVERY PARTICIPANT BLOCKED AND NONE SLEEPING IS A DEADLOCK, and the clock must
// return rather than invent a deadline to advance to. The run unwinds through
// Interrupt instead.
func TestVirtualClockAllBlockedNoneSleepingDoesNotAdvance(t *testing.T) {
	c := NewVirtualClock(epoch)
	p := c.Join("solo")
	defer p.Leave()

	c.Blocked(1)
	if got := c.Now(); !got.Equal(epoch) {
		t.Errorf("Now() = %v; the clock advanced with nothing sleeping", got)
	}
	c.Blocked(-1)
}
