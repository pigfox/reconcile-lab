package reconcilelab

import (
	"context"
	"errors"
	"math/rand/v2"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

// measuredLifetime is the virtual session length the comparison tests run. It is
// shorter than the page's default only because a test does not need forty-five
// seconds of evidence to show three arms separating — and it costs no wall-clock
// time either way, because the clock is virtual.
const measuredLifetime = 5 * time.Second

// runVirtual runs one session to completion on a virtual clock and returns its
// final snapshot. It takes no wall-clock time.
func runVirtual(t *testing.T, cfg Config) (Snapshot, StopReason) {
	t.Helper()

	lab, err := New(cfg, NewVirtualClock(epoch))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer lab.Close()

	reason := lab.Run(t.Context())
	return lab.Snapshot(), reason
}

// --- the comparison the demo is built on -------------------------------------

// THE THREE ARMS MUST LAND IN DIFFERENT PLACES UNDER THE SHIPPED DEFAULT. This
// is the empirical half of the contended-default guard; the static half is
// TestShippedDefaultIsContended in config_test.go. If these three collapse onto
// each other the demo has nothing to show, and no amount of page copy fixes it.
//
// The table is logged rather than only asserted, because the numbers are the
// finding: a later change that keeps them merely distinguishable while moving
// them somewhere uninteresting should be visible in a test run without anyone
// having to reproduce it by hand.
func TestTheThreePoliciesLandInDifferentPlaces(t *testing.T) {
	type arm struct {
		policy Policy
		snap   Snapshot
	}
	arms := make([]arm, 0, len(Policies()))

	for _, p := range Policies() {
		cfg := defaultConfig()
		cfg.Policy = p
		cfg.Lifetime = measuredLifetime

		snap, reason := runVirtual(t, cfg)
		if reason != StopLifetime {
			t.Fatalf("%s: the session ended as %q, want it to run out its lifetime", p, reason)
		}
		arms = append(arms, arm{policy: p, snap: snap})

		t.Logf("%-14s attempts=%-6d landed=%-6d conflicts=%-6d wasted=%.3f  passes=%-7d "+
			"drifts=%-5d restores=%-5d diverged=%-2d p50=%-8v p95=%-8v stoodDown=%-5d gaveUp=%d",
			p, snap.WriteAttempts, snap.WritesLanded, snap.Conflicts, snap.WastedWriteRatio,
			snap.ReconcilePasses, snap.DriftEvents, snap.Restores, snap.Diverged,
			snap.P50, snap.P95, snap.StoodDown, snap.GaveUp)
	}

	for _, a := range arms {
		if a.snap.Conflicts == 0 {
			t.Errorf("%s: no conflicts at all — the default is not contended and the three arms cannot differ", a.policy)
		}
		if a.snap.DriftEvents == 0 {
			t.Errorf("%s: the competing writer never moved an object off target", a.policy)
		}
		if a.snap.Restores == 0 {
			t.Errorf("%s: the loop never restored anything, so this arm shows failure rather than a policy", a.policy)
		}
	}

	byPolicy := map[Policy]Snapshot{}
	for _, a := range arms {
		byPolicy[a.policy] = a.snap
	}
	now, backoff, stand := byPolicy[PolicyRetryNow], byPolicy[PolicyRetryBackoff], byPolicy[PolicyStandDown]

	// RETRY-NOW SPENDS THE MOST AND STAND-DOWN THE LEAST. This is the axis the
	// page's first claim rests on, and it is monotone by construction: one arm
	// keeps trying, one waits between tries, one stops after the first refusal.
	if now.WriteAttempts <= stand.WriteAttempts {
		t.Errorf("retry-now attempted %d writes and stand-down %d — the arm that keeps trying must try more",
			now.WriteAttempts, stand.WriteAttempts)
	}
	if stand.StoodDown == 0 {
		t.Error("stand-down never stood down, so that arm did not exercise its own policy")
	}
	if now.StoodDown != 0 || backoff.StoodDown != 0 {
		t.Errorf("a retrying arm recorded a stand-down (retry-now %d, backoff %d)", now.StoodDown, backoff.StoodDown)
	}

	// AND STAND-DOWN IS THE SLOWEST BACK ON TARGET, because nothing it does can
	// beat the sweep that hands it the key again.
	if stand.P95 <= now.P95 {
		t.Errorf("stand-down's p95 restore is %v against retry-now's %v — the arm bounded below by the "+
			"sweep interval must be the slower one", stand.P95, now.P95)
	}

	// THE THREE WASTED-WRITE RATIOS MUST ACTUALLY BE THREE NUMBERS. Distinct
	// labels over one number is the failure this whole test exists to catch.
	ratios := []float64{now.WastedWriteRatio, backoff.WastedWriteRatio, stand.WastedWriteRatio}
	for i := range ratios {
		for j := i + 1; j < len(ratios); j++ {
			if diff := ratios[i] - ratios[j]; diff < minRatioSeparation && diff > -minRatioSeparation {
				t.Errorf("wasted-write ratios %.4f and %.4f are indistinguishable; the arms have collapsed "+
					"onto one another and the comparison is not one", ratios[i], ratios[j])
			}
		}
	}
}

// minRatioSeparation is how far apart two arms' wasted-write ratios must be
// before they count as different answers rather than one answer with three
// labels.
const minRatioSeparation = 0.01

// --- the accounting has to close ---------------------------------------------

// EVERY DIVERGENCE IS EITHER CLOSED OR STILL OPEN, and nothing else. If this
// identity ever failed, a restore latency would be being attributed to the wrong
// divergence and every percentile on the page would be quietly wrong.
func TestDivergenceAccountingCloses(t *testing.T) {
	cfg := defaultConfig()
	cfg.Lifetime = measuredLifetime

	snap, _ := runVirtual(t, cfg)

	if got, want := snap.Restores+uint64(snap.Diverged), snap.DriftEvents; got != want {
		t.Errorf("restores(%d) + diverged(%d) = %d, want driftEvents %d",
			snap.Restores, snap.Diverged, got, want)
	}
	if got, want := snap.WritesLanded+snap.Conflicts+snap.Rejected, snap.WriteAttempts; got != want {
		t.Errorf("landed(%d) + conflicts(%d) + rejected(%d) = %d, want attempts %d",
			snap.WritesLanded, snap.Conflicts, snap.Rejected, got, want)
	}
	if got, want := snap.DriftLanded+snap.DriftConflicts, snap.DriftAttempts; got != want {
		t.Errorf("drift landed(%d) + conflicts(%d) = %d, want drift attempts %d",
			snap.DriftLanded, snap.DriftConflicts, got, want)
	}
	if snap.Rejected != 0 {
		t.Errorf("Rejected = %d; a refusal that was not a conflict means an invariant broke", snap.Rejected)
	}
	if snap.ReadErrors != 0 {
		t.Errorf("ReadErrors = %d; the key set is fixed at construction and nothing deletes", snap.ReadErrors)
	}
}

// THE COMPETING WRITER LOSES RACES TOO. A writer that could not lose would be a
// scripted event source rather than a competitor, and the contention would be
// something the simulation arranged rather than something it produced.
func TestTheCompetingWriterAlsoLosesRaces(t *testing.T) {
	cfg := defaultConfig()
	cfg.Lifetime = measuredLifetime

	snap, _ := runVirtual(t, cfg)
	if snap.DriftConflicts == 0 {
		t.Error("the competing writer never lost a race — it is not contending, it is dictating")
	}
}

// A CONTROLLER IS NOTIFIED OF ITS OWN WRITES, and the passes that finds nothing
// to do are real work a real controller does. Suppressing them would make the
// loop look cheaper than one.
func TestPassesRunAheadOfWriteAttempts(t *testing.T) {
	cfg := defaultConfig()
	cfg.Lifetime = measuredLifetime

	snap, _ := runVirtual(t, cfg)
	if snap.ReconcilePasses <= snap.WriteAttempts {
		t.Errorf("passes(%d) did not exceed write attempts(%d); the self-notification a real controller "+
			"receives is missing", snap.ReconcilePasses, snap.WriteAttempts)
	}
}

// --- construction -------------------------------------------------------------

func TestNewAppliesTheCeilingsAndReportsThem(t *testing.T) {
	lab, err := New(Config{
		Objects: MaxObjects + 1, DriftRate: MaxDriftRate + 1, Workers: MaxWorkers + 1,
		SweepInterval: MaxSweepInterval + time.Second, Policy: "incinerate", Lifetime: MaxLifetime + time.Second,
	}, NewVirtualClock(epoch))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer lab.Close()

	cfg := lab.Config()
	if cfg.Objects != MaxObjects || cfg.DriftRate != MaxDriftRate || cfg.Workers != MaxWorkers {
		t.Errorf("the ceilings were not applied: %+v", cfg)
	}
	if cfg.Policy != PolicyRetryNow {
		t.Errorf("Policy = %q, want the default after an unknown value", cfg.Policy)
	}
	if len(lab.Clamps()) == 0 {
		t.Error("every ceiling was applied and none was reported")
	}
	if lab.Store().Len() != MaxObjects {
		t.Errorf("the store holds %d objects, want %d", lab.Store().Len(), MaxObjects)
	}
}

func TestNewSeedsEveryObjectOnTarget(t *testing.T) {
	cfg := defaultConfig()
	lab, err := New(cfg, NewVirtualClock(epoch))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer lab.Close()

	for i := range cfg.Objects {
		obj, err := lab.Store().Get(objectName(i))
		if err != nil {
			t.Fatalf("Get %s: %v", objectName(i), err)
		}
		if Replicas(obj) != targetReplicas {
			t.Errorf("%s seeded at %d replicas, want the target %d — the first divergence a run reports "+
				"must be one the competing writer caused", objectName(i), Replicas(obj), targetReplicas)
		}
	}
	if snap := lab.Snapshot(); snap.Diverged != 0 || !snap.Converged() {
		t.Errorf("a freshly seeded lab reports %d diverged objects", snap.Diverged)
	}
}

func TestNewRefusesWhenTheGlobalBudgetIsFull(t *testing.T) {
	_, release, err := globalWorkerBudget.reserve(MaxGlobalWorkers)
	if err != nil {
		t.Fatalf("reserving the whole budget: %v", err)
	}
	defer release()

	lab, err := New(defaultConfig(), NewVirtualClock(epoch))
	if !errors.Is(err, ErrNoWorkerCapacity) {
		t.Errorf("New against a full budget returned %v, want ErrNoWorkerCapacity", err)
	}
	if lab != nil {
		lab.Close()
		t.Error("New returned a Lab alongside its error")
	}
}

func TestNewClampsThePoolToWhatTheBudgetHasLeft(t *testing.T) {
	_, release, err := globalWorkerBudget.reserve(MaxGlobalWorkers - 1)
	if err != nil {
		t.Fatalf("reserving all but one: %v", err)
	}
	defer release()

	cfg := defaultConfig()
	cfg.Workers = MaxWorkers
	lab, err := New(cfg, NewVirtualClock(epoch))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer lab.Close()

	if lab.Config().Workers != 1 {
		t.Errorf("Workers = %d, want the 1 the budget had left", lab.Config().Workers)
	}
	var named bool
	for _, c := range lab.Clamps() {
		if c.Field == fieldWorkers {
			named = true
		}
	}
	if !named {
		t.Errorf("the pool was clamped by the budget and not reported: %+v", lab.Clamps())
	}
}

// THE SEEDING FAILURE PATH. It is unreachable through New — the store is always
// fresh and the names are generated uniquely — so it is driven through the
// injection seam. A path no test can reach is a path nobody has checked.
func TestNewReportsASeedingFailureAndGivesTheBudgetBack(t *testing.T) {
	store := NewStore()
	if _, err := store.Create(NewDeployment(objectName(0), targetReplicas)); err != nil {
		t.Fatalf("pre-seeding the collision: %v", err)
	}

	before := GlobalWorkersInUse()
	lab, err := newLab(defaultConfig(), NewVirtualClock(epoch), store)
	if err == nil {
		lab.Close()
		t.Fatal("seeding onto a colliding name succeeded")
	}
	if !apierrors.IsAlreadyExists(errors.Unwrap(err)) {
		t.Errorf("the seeding error was %v, want a wrapped AlreadyExists", err)
	}
	if got := GlobalWorkersInUse(); got != before {
		t.Errorf("workers in use = %d after a failed construction, want %d — the reservation leaked", got, before)
	}
}

func TestCloseIsIdempotent(t *testing.T) {
	before := GlobalWorkersInUse()

	lab, err := New(defaultConfig(), NewVirtualClock(epoch))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	lab.Close()
	lab.Close()

	if got := GlobalWorkersInUse(); got != before {
		t.Errorf("workers in use = %d after a double Close, want %d", got, before)
	}
}

// --- how a session ends --------------------------------------------------------

func TestRunEndsAtItsLifetime(t *testing.T) {
	cfg := defaultConfig()
	cfg.Lifetime = MinLifetime

	_, reason := runVirtual(t, cfg)
	if reason != StopLifetime {
		t.Errorf("reason = %q, want %q", reason, StopLifetime)
	}
}

// A CANCELED RUN UNWINDS WITHOUT WAITING OUT ITS CEILING, and this proves it
// STRUCTURALLY rather than by winning a race: an extra participant is joined and
// never waits, so the clock can never reach quiescence and the lifetime sleep
// can never fire. Only the cancellation can end this session.
func TestRunEndsWhenTheCallerGoesAway(t *testing.T) {
	clock := NewVirtualClock(epoch)
	holder := clock.Join("holder-that-never-waits")

	cfg := defaultConfig()
	cfg.Lifetime = MaxLifetime
	lab, err := New(cfg, clock)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer lab.Close()

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	reason := lab.Run(ctx)
	if reason != StopCanceled {
		t.Errorf("reason = %q, want %q", reason, StopCanceled)
	}
	holder.Leave()
}

// A STREAM THAT JUST STOPS LOOKS LIKE A NETWORK FAILURE, so both endings carry a
// sentence the page can render as-is.
func TestEveryStopReasonCarriesASentence(t *testing.T) {
	for _, r := range []StopReason{StopLifetime, StopCanceled} {
		if msg := r.Message(); msg == "" {
			t.Errorf("%q carries no message", r)
		}
	}
	if StopLifetime.Message() == StopCanceled.Message() {
		t.Error("the two endings read identically, so a visitor cannot tell them apart")
	}
}

// --- the pieces, driven directly ------------------------------------------------

// newIdleLab builds a Lab that is never run, so its internals can be driven
// directly. The caller must Close it.
func newIdleLab(t *testing.T, objects int) *Lab {
	t.Helper()
	cfg := defaultConfig()
	cfg.Objects = objects
	lab, err := New(cfg, NewVirtualClock(epoch))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return lab
}

func TestReconcileRecordsAReadFailure(t *testing.T) {
	lab := newIdleLab(t, 1)
	defer lab.Close()

	p := lab.clock.Join("probe")
	lab.reconcile(p, "no-such-object")
	p.Leave()

	if got := lab.Snapshot().ReadErrors; got != 1 {
		t.Errorf("ReadErrors = %d after reconciling an absent key, want 1", got)
	}
}

// AN UNKNOWN POLICY MUST NOT SILENTLY BEHAVE LIKE ONE OF THE THREE. Normalize is
// the only door and it substitutes a valid policy, so this state is unreachable
// through New — which is exactly why it is worth pinning that the fallback stops
// rather than guesses. A fallback that fell through to the next loop iteration
// would be retry-now wearing another name, and nothing on the page would say so.
//
// It is driven through a REAL RUN rather than a hand-made conflict, because a
// conflict is what this whole package refuses to manufacture: the contended
// default produces them, so setting an invalid policy on a running session
// reaches the branch the same way a real one would.
func TestAnUnknownPolicyStopsRatherThanGuessing(t *testing.T) {
	cfg := defaultConfig()
	cfg.Lifetime = measuredLifetime

	lab, err := New(cfg, NewVirtualClock(epoch))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer lab.Close()

	// Past Normalize, which is the only reason this is reachable at all.
	lab.cfg.Policy = "incinerate"

	if reason := lab.Run(t.Context()); reason != StopLifetime {
		t.Fatalf("the session ended as %q, want it to run out its lifetime", reason)
	}
	snap := lab.Snapshot()

	if snap.Conflicts == 0 {
		t.Fatal("no conflicts arose, so the policy branch was never reached and this proves nothing")
	}
	if snap.StoodDown != 0 {
		t.Errorf("StoodDown = %d — an unknown policy was counted as stand-down", snap.StoodDown)
	}
	if snap.GaveUp != 0 {
		t.Errorf("GaveUp = %d — an unknown policy fell through to a retrying arm and exhausted the cap", snap.GaveUp)
	}
}

// THE REJECTED PATH. A refusal that is not a conflict is structurally
// unreachable in a running session, and it is counted rather than ignored so
// that a change which made it reachable would show up as a number rather than as
// silence.
func TestAWriteRefusedForAnyOtherReasonIsCountedSeparately(t *testing.T) {
	lab := newIdleLab(t, 1)
	defer lab.Close()

	absent := NewDeployment("never-created", targetReplicas)
	if got := lab.applyLoopWrite(absent); got != writeRejected {
		t.Errorf("outcome = %v, want writeRejected", got)
	}
	if got := lab.applyDriftWrite(absent); got != writeRejected {
		t.Errorf("drift outcome = %v, want writeRejected", got)
	}

	snap := lab.Snapshot()
	if snap.Rejected != 2 {
		t.Errorf("Rejected = %d, want 2", snap.Rejected)
	}
	if snap.Conflicts != 0 || snap.DriftConflicts != 0 {
		t.Error("a refusal that was not a conflict was counted as one")
	}
}

// THE TRANSITIONS, DRIVEN ONE AT A TIME. The bookkeeping has to be atomic with
// the write it describes, and the three cases it distinguishes are what every
// percentile on the page is built from.
func TestDivergenceTransitions(t *testing.T) {
	lab := newIdleLab(t, 1)
	defer lab.Close()

	key := objectName(0)
	off := NewDeployment(key, driftValues[0])
	on := NewDeployment(key, targetReplicas)

	lab.mu.Lock()
	defer lab.mu.Unlock()

	// On target while nothing was diverged: nothing to close, nothing counted.
	lab.noteWriteLocked(on)
	if lab.restores != 0 || lab.driftEvents != 0 {
		t.Errorf("an on-target write onto a converged object counted something: restores=%d drifts=%d",
			lab.restores, lab.driftEvents)
	}

	// Off target: a divergence opens.
	lab.noteWriteLocked(off)
	if lab.driftEvents != 1 || len(lab.divergedAt) != 1 {
		t.Errorf("driftEvents=%d diverged=%d, want 1 and 1", lab.driftEvents, len(lab.divergedAt))
	}
	opened := lab.divergedAt[key]

	// Off target again: superseded, and the clock on the divergence must NOT
	// restart — the figure being measured is how long the object was wrong.
	lab.noteWriteLocked(off)
	if lab.superseded != 1 {
		t.Errorf("superseded = %d, want 1", lab.superseded)
	}
	if lab.driftEvents != 1 {
		t.Errorf("driftEvents = %d after a superseding write, want it to stay at 1", lab.driftEvents)
	}
	if !lab.divergedAt[key].Equal(opened) {
		t.Error("a superseding write restarted the divergence clock; the restore latency would understate")
	}

	// Back on target: the divergence closes and is observed.
	lab.noteWriteLocked(on)
	if lab.restores != 1 || len(lab.divergedAt) != 0 {
		t.Errorf("restores=%d diverged=%d, want 1 and 0", lab.restores, len(lab.divergedAt))
	}
	if lab.hist.total != 1 {
		t.Errorf("the histogram holds %d samples, want 1", lab.hist.total)
	}
}

// THE REQUEUE SET DEDUPLICATES BY KEY, which is what a controller's work queue
// does: a burst of ten events on one object costs one reconcile rather than ten.
func TestRequeueSetDeduplicates(t *testing.T) {
	lab := newIdleLab(t, 2)
	defer lab.Close()

	lab.mu.Lock()
	for range 10 {
		lab.enqueueLocked(objectName(0))
	}
	lab.enqueueLocked(objectName(1))
	depth := len(lab.order)
	lab.mu.Unlock()

	if depth != 2 {
		t.Errorf("the requeue set holds %d entries after eleven enqueues of two keys, want 2", depth)
	}
	if got := lab.Snapshot().MaxQueueDepth; got != 2 {
		t.Errorf("MaxQueueDepth = %d, want 2", got)
	}

	first, ok := lab.nextKey(0)
	if !ok || first != objectName(0) {
		t.Errorf("nextKey = %q,%v — the set must hand back keys in arrival order", first, ok)
	}
}

// THE COMPETING WRITER UNWINDS ON A READ IT CANNOT SATISFY. Structurally
// unreachable in a running session, for the same reason the loop's read-error
// branch is: the key set is fixed at construction and nothing deletes. It is
// driven directly rather than left unchecked.
func TestDriftUnwindsWhenItsObjectIsGone(t *testing.T) {
	lab := newIdleLab(t, 1)
	defer lab.Close()

	lab.keys = []string{"no-such-object"}
	p := lab.clock.Join("probe")
	rng := rand.New(rand.NewPCG(defaultSeed, pcgStream))

	if lab.driftOnce(p, rng) {
		t.Error("the competing writer carried on after a read it could not satisfy")
	}
	p.Leave()

	if got := lab.Snapshot().DriftAttempts; got != 0 {
		t.Errorf("DriftAttempts = %d — a write was attempted against an object that was never read", got)
	}
}

// THE SWEEP UNWINDS WHEN THE SESSION HAS ALREADY ENDED. halt() can land between
// two sweeps, and a sweeper that kept enqueueing into a stopped session would
// hold the run open past its own ending.
func TestSweepUnwindsAfterTheSessionEnds(t *testing.T) {
	lab := newIdleLab(t, 1)
	defer lab.Close()

	lab.halt()

	p := lab.clock.Join("probe")
	lab.sweep(p) // returns rather than looping, because enqueueAll refuses
	p.Leave()

	if got := lab.Snapshot().QueueDepth; got != 0 {
		t.Errorf("QueueDepth = %d after a sweep of a stopped session, want 0", got)
	}
}

func TestSweepHandsBackEveryKey(t *testing.T) {
	lab := newIdleLab(t, 3)
	defer lab.Close()

	if !lab.enqueueAll() {
		t.Fatal("enqueueAll reported the session had ended before it started")
	}
	if got := lab.Snapshot().QueueDepth; got != 3 {
		t.Errorf("QueueDepth = %d after a sweep of three objects, want 3", got)
	}

	lab.halt()
	if lab.enqueueAll() {
		t.Error("a sweep after the session ended still enqueued")
	}
}

func TestHaltIsIdempotentAndReleasesWaiters(t *testing.T) {
	lab := newIdleLab(t, 1)
	defer lab.Close()

	lab.halt()
	lab.halt()

	if _, ok := lab.nextKey(0); ok {
		t.Error("nextKey handed out a key after the session ended")
	}
}

// --- the competing writer's own machinery ---------------------------------------

func TestJitterStaysWithinItsBand(t *testing.T) {
	rng := rand.New(rand.NewPCG(defaultSeed, pcgStream))
	base := 100 * time.Millisecond

	low := time.Duration(float64(base) * (1 - driftJitterFraction))
	high := time.Duration(float64(base) * (1 + driftJitterFraction))
	for range 1000 {
		got := jitter(rng, base)
		if got < low || got > high {
			t.Fatalf("jitter returned %v, outside [%v, %v]", got, low, high)
		}
	}
}

// A JITTERED SLEEP MUST STILL BE A SLEEP. A zero or negative duration would
// return immediately and the competing writer would spin.
func TestJitterNeverReturnsANonSleep(t *testing.T) {
	rng := rand.New(rand.NewPCG(defaultSeed, pcgStream))
	for range 1000 {
		if got := jitter(rng, 1); got < 1 {
			t.Fatalf("jitter of the smallest possible tick returned %v", got)
		}
	}
}

// EVERY VALUE THE COMPETING WRITER CHOOSES IS OFF TARGET. If one of them equaled
// the target, some fraction of competing writes would land in agreement with the
// loop, would not count as divergences, and the contention on screen would be
// overstated — wrong in the flattering direction.
func TestEveryDriftValueIsOffTarget(t *testing.T) {
	if len(driftValues) == 0 {
		t.Fatal("the competing writer has no values to choose from")
	}
	for _, v := range driftValues {
		if v == targetReplicas {
			t.Errorf("the competing writer can write %d, which is the target — that write would move nothing", v)
		}
	}
}

// THE RUN IS REPRODUCIBLE FROM ITS CONFIG, AND THE WHOLE SNAPSHOT IS, NOT A
// CHOSEN FEW COUNTERS. A figure on this page can otherwise only be believed
// rather than checked, and "believed" is not what this estate ships.
//
// IT COMPARES THE ENTIRE STRUCT with ==, which is available because every field
// of Snapshot is a comparable scalar. That is deliberate rather than convenient:
// a test that listed the fields it cared about would silently stop covering any
// field added later, and the next non-deterministic number would be the one
// nobody thought to name.
//
// TWENTY RUNS, not two. A scheduler-dependent difference does not show up
// reliably in one comparison — the whole point of the virtual clock is that at
// most one goroutine is runnable at a time, and a defect in that property would
// be intermittent by nature.
func TestTwentyRunsOfOneConfigAreByteIdentical(t *testing.T) {
	cfg := defaultConfig()
	cfg.Lifetime = measuredLifetime
	cfg.Seed = 7

	first, _ := runVirtual(t, cfg)
	for i := 1; i < determinismRuns; i++ {
		got, _ := runVirtual(t, cfg)
		if got != first {
			t.Fatalf("run %d differed from run 0:\n  run 0: %+v\n  run %d: %+v", i, first, i, got)
		}
	}

	// Refuse the vacuous pass: a snapshot in which nothing happened would compare
	// equal every time and prove nothing about determinism.
	if first.WriteAttempts == 0 || first.Conflicts == 0 || first.Restores == 0 {
		t.Fatalf("the runs being compared did no work, so their agreement proves nothing: %+v", first)
	}
}

// determinismRuns is how many identical runs are compared. The directive that
// authorized this demo asked for at least twenty.
const determinismRuns = 20

func TestADifferentSeedGivesADifferentRun(t *testing.T) {
	cfg := defaultConfig()
	cfg.Lifetime = measuredLifetime

	cfg.Seed = 1
	first, _ := runVirtual(t, cfg)
	cfg.Seed = 2
	second, _ := runVirtual(t, cfg)

	if first.Conflicts == second.Conflicts && first.WriteAttempts == second.WriteAttempts {
		t.Error("two different seeds produced identical counters; the seed is not reaching the writer")
	}
}

func TestObjectNamesAreDistinct(t *testing.T) {
	seen := map[string]bool{}
	for i := range 100 {
		name := objectName(i)
		if seen[name] {
			t.Fatalf("objectName(%d) = %q, which was already used", i, name)
		}
		seen[name] = true
	}
}

// --- the snapshot ---------------------------------------------------------------

func TestSnapshotRatesAreZeroBeforeAnythingHappens(t *testing.T) {
	lab := newIdleLab(t, 1)
	defer lab.Close()

	snap := lab.Snapshot()
	if snap.ConflictRate != 0 || snap.LandedRate != 0 {
		t.Errorf("rates over a zero-length session are non-zero: %+v", snap)
	}
	if snap.WastedWriteRatio != 0 {
		t.Errorf("WastedWriteRatio = %v before any attempt, want 0 — the page renders this as a dash "+
			"rather than as a perfect score", snap.WastedWriteRatio)
	}
	if !snap.Converged() {
		t.Error("a freshly seeded lab reports itself unconverged")
	}
}

func TestSnapshotReportsTheShapeThatIsActuallyRunning(t *testing.T) {
	cfg := defaultConfig()
	lab, err := New(cfg, NewVirtualClock(epoch))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer lab.Close()

	snap := lab.Snapshot()
	if snap.Objects != cfg.Objects || snap.Workers != cfg.Workers {
		t.Errorf("Snapshot reports objects=%d workers=%d, want %d and %d",
			snap.Objects, snap.Workers, cfg.Objects, cfg.Workers)
	}
}
