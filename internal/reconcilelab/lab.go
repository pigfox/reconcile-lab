// Package reconcilelab is the simulation behind /demos/reconcile-lab: a
// Kubernetes-style reconcile loop and a competing writer contending for the same
// objects, with the loop's answer to a refused write as the only thing that
// varies.
//
// WHAT THE DEMO IS ARGUING. Every write to a Kubernetes object carries the
// version it was read at, and the API server refuses the one that lost the race.
// A controller therefore has to decide what to do about a refusal, and there are
// only three answers: try again at once, try again after waiting, or hand the
// key back and wait to be asked again. Under the same contention the three pay
// in different currencies — wasted writes against time spent off target — and
// past a certain contention the cheapest answer becomes the most expensive one.
// That crossing is the demo.
//
// THE CONTENTION IS EMERGENT AND NOTHING INJECTS IT. There is no reactor, no
// interceptor and no schedule of planted conflicts. A second goroutine writes
// the same objects on its own jittered cadence, the store enforces the version,
// and every conflict this page reports is one that a race actually produced.
// That constraint is why there is no fake client here: PF-S193b measured
// client-go's fake object tracker assigning no resourceVersion at all and
// accepting stale writes, so every conflict on that substrate would have had to
// be staged.
//
// THIS PACKAGE IS PURE GO. It has no HTTP surface, no template and no browser
// dependency: the transport lives in internal/app, and the page lives in a
// template. So the simulation is testable on the host, at full speed, with no
// server and no network.
//
// TIME IS INJECTED, and that is what makes the tests deterministic AND fast. A
// live session runs on NewRealClock and paces itself against the wall so a
// visitor watches conflicts accumulate in real time; the same code under
// VirtualClock runs a ninety-second session in microseconds and produces the
// same numbers, because virtual time only moves when every participating
// goroutine is waiting. No test in this package sleeps. See clock.go for why
// every store call is charged a simulated round trip, which is the one thing
// that makes an unthrottled retry policy expressible on a virtual clock at all.
//
// THE RUN IS REPRODUCIBLE FROM ITS CONFIG. The competing writer's jitter comes
// from a seeded generator and the seed is part of the Config, so the same
// controls give the same run. A figure on this page can be checked by someone
// else rather than merely believed.
//
// THE CEILINGS ARE ENFORCED HERE, NOT AT THE EDGE. See constants.go: Normalize
// is the only door into a Config, and the global worker budget is committed by
// this package before a single goroutine starts. A limit the transport owns is a
// limit the second transport does not have.
package reconcilelab

import (
	"context"
	"fmt"
	"math/rand/v2"
	"strconv"
	"sync"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

// StopReason says why a session ended. It reaches the visitor in the stream's
// final frame — a stream that just stops looks like a network failure, and a
// visitor who cannot tell the two apart will assume the demo broke.
type StopReason string

const (
	// StopLifetime is the server closing the session at its own ceiling.
	StopLifetime StopReason = "lifetime"
	// StopCanceled is the client going away, or the process shutting down.
	StopCanceled StopReason = "canceled"
)

// Message is what the reason means, in a sentence the page can render as-is.
func (r StopReason) Message() string {
	if r == StopLifetime {
		return "The server closed this session at its " + MaxLifetime.String() +
			" ceiling. Start another one to keep going."
	}
	return "The session ended because the connection did."
}

// writeOutcome is what one compare-and-swap attempt did. It is an enum rather
// than an error because the only distinction any caller acts on is "landed" or
// "lost the race", and returning the error would invite a caller to branch on
// its text.
type writeOutcome int

const (
	// writeLanded means the store accepted the write.
	writeLanded writeOutcome = iota
	// writeConflicted means another writer got there first.
	writeConflicted
	// writeRejected is any other refusal. It is structurally unreachable in a
	// running session — the key set is fixed at construction and nothing deletes
	// — and is counted rather than ignored so that a change which made it
	// reachable would show up as a number instead of as silence.
	writeRejected
)

// Snapshot is the simulation's state at one instant. It is a value, copied out
// under the lock, so a caller can serialize it without holding anything.
//
// EVERY FIELD HERE IS A MEASUREMENT. The settings a visitor chose — object
// count, drift rate, worker count, sweep interval, policy — are NOT here; they
// travel in the config frame instead. Putting an input next to a result in the
// same struct is how a page ends up printing the number it was given as though
// the run had produced it.
type Snapshot struct {
	Elapsed time.Duration

	// WriteAttempts is every compare-and-swap the reconcile loop tried.
	// WritesLanded is how many the store accepted. The difference is the work
	// the loop did that bought nothing, and under the retry-now policy it is the
	// figure that grows fastest.
	WriteAttempts uint64
	WritesLanded  uint64
	// Conflicts is attempts the store refused because the object had moved. It
	// is emergent: no code path in this package creates one on purpose.
	Conflicts uint64
	// Rejected is refusals that were not conflicts. See writeRejected — a
	// non-zero value here means an invariant broke, not that the load was heavy.
	Rejected uint64

	// ReconcilePasses is how many times the loop read an object and decided what
	// to do about it, including the passes that found nothing to do.
	ReconcilePasses uint64
	// StoodDown is passes abandoned on the first refusal, and GaveUp is passes
	// that exhausted the retry cap. Both hand the key back; the difference is
	// what they spent first.
	StoodDown uint64
	GaveUp    uint64
	// ReadErrors is reads that failed. Structurally unreachable, reported for
	// the same reason as Rejected.
	ReadErrors uint64

	// DriftEvents is how many times a landed competing write moved an object
	// from on-target to off-target. Superseded is a further competing write
	// landing on an object that was ALREADY off target — it does not start a new
	// divergence, so it is counted separately rather than inflating the first
	// figure. DriftConflicts is the competing writer losing a race to the loop,
	// which is the same mechanism seen from the other side.
	DriftEvents    uint64
	Superseded     uint64
	DriftAttempts  uint64
	DriftLanded    uint64
	DriftConflicts uint64

	// Restores is divergences the loop closed. Diverged is how many objects are
	// off target right now.
	Restores uint64
	Diverged int

	// Objects and Workers are the shape the simulation is actually running with,
	// after every ceiling and the global budget have been applied. They are here
	// rather than in the config frame alone because a clamped pool is a fact
	// about the run.
	Objects int
	Workers int

	// QueueDepth is the requeue set right now and MaxQueueDepth is its high
	// water mark. The set DEDUPLICATES by key, which is what a controller's work
	// queue does, so this is a count of distinct objects waiting rather than of
	// events received.
	QueueDepth    int
	MaxQueueDepth int

	// ConflictRate and LandedRate are measured over the session so far, in
	// events per second — not the requested figures.
	ConflictRate float64
	LandedRate   float64

	// DriftRateAchieved is how many competing writes actually landed per second.
	//
	// IT IS A MEASUREMENT AND IT IS ON THE PAGE BESIDE THE REQUESTED RATE, which
	// is not redundancy. A single writer cannot exceed one write per round-trip
	// pair no matter what it is asked for, and it also loses races, so the two
	// figures genuinely differ. Showing only the requested one would be printing
	// an input as though the run had produced it.
	DriftRateAchieved float64

	// WastedWriteRatio is refused attempts over all attempts, in [0,1]. It is
	// zero when nothing has been attempted yet, which the page renders as a dash
	// rather than as a perfect score.
	WastedWriteRatio float64

	// The restore-latency percentiles, from the write that moved an object off
	// target to the write that brought it back. They come from a fixed bucket
	// ladder, so each is the upper bound of the bucket the quantile falls in and
	// never understates. Zero means nothing has been restored yet.
	P50 time.Duration
	P95 time.Duration
	P99 time.Duration
	Max time.Duration
}

// Converged reports whether every object is on target at this instant.
func (s Snapshot) Converged() bool { return s.Diverged == 0 }

// Lab is one session's simulation.
//
// IT OWNS EVERY STORE CALL AND MAKES THEM ALL UNDER ITS OWN LOCK. That is
// heavier than it needs to be for the store's sake — Store is safe on its own —
// and it is deliberate: a landed write and the divergence bookkeeping that
// describes it have to be one atomic step, or two writers finishing at the same
// instant could record their transitions in the opposite order and the restore
// latencies would be attributed to the wrong divergence. The lock order is
// always Lab then Store, and the Store never calls back, so the pair cannot
// deadlock. Every sleep happens OUTSIDE the lock.
type Lab struct {
	cfg     Config
	clamps  []Clamp
	clock   Clock
	release func()

	mu    sync.Mutex
	store *Store
	keys  []string

	// The requeue set: a FIFO that deduplicates by key, which is what a
	// controller's work queue does. queued is membership, order is arrival.
	queued map[string]bool
	order  []string

	// parked holds one slot per reconcile worker: a channel when that worker is
	// waiting for work, nil when it is running. A woken worker is handed its KEY
	// down the channel rather than merely signaled.
	//
	// THIS SHAPE WAS ARRIVED AT BY MEASUREMENT, TWICE, and both intermediate
	// versions looked correct.
	//
	// The FIRST version parked workers on a shared sync.Cond and woke them with
	// Broadcast, which releases every waiter at once and leaves the scheduler to
	// decide which reaches the head of the requeue set. Twenty runs of one config
	// gave two answers — ReconcilePasses 2406 against 2407, GaveUp 3 against 2,
	// every other field identical.
	//
	// The SECOND version replaced that with a FIFO of per-waiter channels, woken
	// one at a time. It made the failure rarer and did not remove it, because
	// SIGNALING A WORKER IS NOT GIVING IT THE WORK: between the signal and the
	// woken worker re-acquiring the mutex, a DIFFERENT worker that was already
	// running could take the lock — Go's mutex is not FIFO — and take the key
	// that was meant for it. The same 2406/2407 signature came back under
	// -race -shuffle=on.
	//
	// So the key is handed over inside the same critical section that enqueued
	// it, and never enters the shared queue at all when a worker is waiting for
	// it. THAT is what removed the scheduler from the decision; the recipient
	// being chosen by worker index rather than arrival order is a second,
	// smaller precaution, and takeParkedLocked records the measurement showing
	// it is not the part that mattered.
	parked []chan string

	// divergedAt holds, for every object currently off target, the instant it
	// went off target. A further drift onto an already-diverged object does not
	// move it — the figure being measured is how long the object was wrong, not
	// how long since the most recent write.
	divergedAt map[string]time.Time

	writeAttempts uint64
	writesLanded  uint64
	conflicts     uint64
	rejected      uint64
	passes        uint64
	stoodDown     uint64
	gaveUp        uint64
	readErrors    uint64

	driftEvents    uint64
	superseded     uint64
	driftAttempts  uint64
	driftLanded    uint64
	driftConflicts uint64
	restores       uint64

	maxQueueDepth int

	hist    *histogram
	started time.Time
	stopped bool

	closeOnce sync.Once
}

// New normalizes the requested config, commits the session's share of the global
// worker budget, seeds the object set, and returns a Lab ready to Run.
//
// The caller MUST Close the Lab, whether or not it ever runs — the worker budget
// is committed here, so a Lab that is created and dropped would leak capacity
// that no later session could reclaim.
func New(req Config, clock Clock) (*Lab, error) {
	return newLab(req, clock, NewStore())
}

// newLab is New with the store supplied. Ordinary dependency injection: it lets
// a test drive the seeding failure path, which is otherwise unreachable because
// New always hands in an empty store and the generated names are unique. A path
// no test can reach is a path nobody has checked.
func newLab(req Config, clock Clock, store *Store) (*Lab, error) {
	cfg, clamps := Normalize(req)

	granted, release, err := globalWorkerBudget.reserve(cfg.Workers)
	if err != nil {
		return nil, err
	}
	if granted < cfg.Workers {
		clamps = append(clamps, Clamp{
			Field:     fieldWorkers,
			Requested: strconv.Itoa(cfg.Workers),
			Applied:   strconv.Itoa(granted),
			Reason: "another session holds part of the " + strconv.Itoa(MaxGlobalWorkers) +
				"-worker budget this server runs in total",
		})
		cfg.Workers = granted
	}

	keys, err := seedStore(store, cfg.Objects)
	if err != nil {
		release()
		return nil, err
	}

	l := &Lab{
		cfg:        cfg,
		clamps:     clamps,
		clock:      clock,
		release:    release,
		store:      store,
		keys:       keys,
		queued:     make(map[string]bool, cfg.Objects),
		parked:     make([]chan string, cfg.Workers),
		divergedAt: make(map[string]time.Time, cfg.Objects),
		hist:       newHistogram(),
		started:    clock.Now(),
	}
	return l, nil
}

// seedStore creates the object set, every object starting ON target so that the
// first divergence a run reports is one the competing writer actually caused.
func seedStore(store *Store, objects int) ([]string, error) {
	keys := make([]string, 0, objects)
	for i := range objects {
		name := objectName(i)
		if _, err := store.Create(NewDeployment(name, targetReplicas)); err != nil {
			return nil, fmt.Errorf("reconcilelab: seeding %s: %w", name, err)
		}
		keys = append(keys, name)
	}
	return keys, nil
}

// objectName is the name of the i-th seeded Deployment.
func objectName(i int) string { return objectNamePrefix + strconv.Itoa(i) }

// objectNamePrefix is where every seeded object's name starts.
const objectNamePrefix = "app-"

// Config is the simulation that will actually run — the requested one after
// every ceiling has been applied.
func (l *Lab) Config() Config { return l.cfg }

// Clamps is every ceiling that changed the request, in the order they were
// applied. Empty means the visitor got exactly what they asked for.
func (l *Lab) Clamps() []Clamp { return l.clamps }

// Store is the object store this session is contending over. It is exposed so a
// test can read the objects back; nothing in the transport uses it.
func (l *Lab) Store() *Store { return l.store }

// Close returns this session's workers to the global budget. It is idempotent.
func (l *Lab) Close() {
	l.closeOnce.Do(func() {
		if l.release != nil {
			l.release()
		}
	})
}

// Run drives the simulation until ctx is done or the lifetime ceiling is
// reached, and returns only after EVERY goroutine it started has exited. That
// join is what makes the leak test in this package meaningful: a caller that has
// seen Run return knows there is nothing left running.
func (l *Lab) Run(ctx context.Context) StopReason {
	l.mu.Lock()
	l.started = l.clock.Now()
	l.mu.Unlock()

	var (
		wg     sync.WaitGroup
		once   sync.Once
		reason StopReason
	)
	stop := make(chan struct{})
	finish := func(r StopReason) {
		once.Do(func() {
			reason = r
			close(stop)
			l.clock.Interrupt()
			l.halt()
		})
	}

	// Join every participant HERE, in this order, before any goroutine starts.
	// VirtualClock breaks deadline ties on join order, so doing it from the one
	// goroutine that spawns them is what keeps a tie from depending on the
	// scheduler.
	life := l.clock.Join("lifetime")
	writer := l.clock.Join("writer")
	sweeper := l.clock.Join("sweeper")
	pool := make([]Participant, l.cfg.Workers)
	for i := range pool {
		pool[i] = l.clock.Join("reconcile-" + strconv.Itoa(i))
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer life.Leave()
		if life.Sleep(l.cfg.Lifetime) {
			finish(StopLifetime)
		}
	}()

	// The cancellation watcher is NOT a clock participant: it never sleeps on
	// simulated time, so counting it would leave the virtual clock permanently
	// one waiter short of quiescent and time would never advance.
	wg.Add(1)
	go func() {
		defer wg.Done()
		select {
		case <-ctx.Done():
			finish(StopCanceled)
		case <-stop:
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer writer.Leave()
		l.drift(writer)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer sweeper.Leave()
		l.sweep(sweeper)
	}()

	for i := range pool {
		p, idx := pool[i], i
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer p.Leave()
			l.work(p, idx)
		}()
	}

	wg.Wait()
	return reason
}

// Snapshot copies the current state out under the lock.
func (l *Lab) Snapshot() Snapshot {
	l.mu.Lock()
	defer l.mu.Unlock()

	elapsed := l.clock.Now().Sub(l.started)
	secs := elapsed.Seconds()

	s := Snapshot{
		Elapsed:         elapsed,
		WriteAttempts:   l.writeAttempts,
		WritesLanded:    l.writesLanded,
		Conflicts:       l.conflicts,
		Rejected:        l.rejected,
		ReconcilePasses: l.passes,
		StoodDown:       l.stoodDown,
		GaveUp:          l.gaveUp,
		ReadErrors:      l.readErrors,
		DriftEvents:     l.driftEvents,
		Superseded:      l.superseded,
		DriftAttempts:   l.driftAttempts,
		DriftLanded:     l.driftLanded,
		DriftConflicts:  l.driftConflicts,
		Restores:        l.restores,
		Diverged:        len(l.divergedAt),
		Objects:         l.cfg.Objects,
		Workers:         l.cfg.Workers,
		QueueDepth:      len(l.order),
		MaxQueueDepth:   l.maxQueueDepth,
		P50:             l.hist.quantile(quantileP50),
		P95:             l.hist.quantile(quantileP95),
		P99:             l.hist.quantile(quantileP99),
		Max:             l.hist.max,
	}
	if secs > 0 {
		s.ConflictRate = float64(l.conflicts) / secs
		s.LandedRate = float64(l.writesLanded) / secs
		s.DriftRateAchieved = float64(l.driftLanded) / secs
	}
	if l.writeAttempts > 0 {
		s.WastedWriteRatio = float64(l.writeAttempts-l.writesLanded) / float64(l.writeAttempts)
	}
	return s
}

// --- the goroutines ---------------------------------------------------------

// drift is the competing writer: a second actor mutating the same objects on its
// own cadence. It stands in for a person at a terminal or another controller
// with a different opinion, and it is the ONLY source of divergence in the
// simulation.
//
// IT IS A FULL COMPARE-AND-SWAP CLIENT, not a privileged mutator. It reads, it
// modifies, it writes back the version it read, and it can LOSE — which is the
// same race the loop is in, seen from the other side, and is why DriftConflicts
// is on the page. A writer that could not lose would be a scripted event source
// rather than a competitor.
//
// IT BATCHES ABOVE ONE WRITE A MILLISECOND, for the reason drifterMinTick
// records, and it jitters each sleep so that it cannot settle into a fixed phase
// relationship with the loop.
func (l *Lab) drift(p Participant) {
	rng := rand.New(rand.NewPCG(l.cfg.Seed, pcgStream))

	// No zero guard on the interval, deliberately: MinDriftRate is one a second
	// and Normalize is the only door in. A defensive branch here would be a
	// statement no test could reach honestly.
	interval := time.Duration(float64(time.Second) / float64(l.cfg.DriftRate))

	// THE WRITER'S OWN ROUND TRIPS COME OUT OF ITS SLEEP, and getting this wrong
	// was measured rather than reasoned. The first version slept the whole
	// interval and THEN spent two round trips writing, so a request for 300
	// writes a second produced 130 — the page would have been naming a rate the
	// simulation never ran at, which is the one thing a control on this site may
	// not do. Sleeping the interval MINUS the write cost makes the requested rate
	// achievable up to the hard ceiling of one write per writeCost, and above
	// that the achieved rate is reported as the measurement it is.
	gap := interval - writeCost
	if gap < drifterMinTick {
		gap = drifterMinTick
	}

	last := l.clock.Now()
	var credit float64
	for {
		if !p.Sleep(jitter(rng, gap)) {
			return
		}
		// Credit is taken from time that ACTUALLY ELAPSED, which includes the
		// round trips spent writing on the previous iteration. That is what makes
		// the achieved rate track the requested one instead of falling behind it
		// by the cost of the work.
		now := l.clock.Now()
		credit += float64(now.Sub(last)) / float64(interval)
		last = now

		due := int(credit)
		credit -= float64(due)

		for range due {
			if !l.driftOnce(p, rng) {
				return
			}
		}
	}
}

// jitter spreads a sleep either side of d by driftJitterFraction.
func jitter(rng *rand.Rand, d time.Duration) time.Duration {
	factor := 1 - driftJitterFraction + rng.Float64()*(2*driftJitterFraction)
	out := time.Duration(float64(d) * factor)
	if out < 1 {
		return 1
	}
	return out
}

// driftOnce performs one competing read-modify-write. It reports false when the
// session has ended and the writer should unwind.
func (l *Lab) driftOnce(p Participant, rng *rand.Rand) bool {
	key := l.keys[rng.IntN(len(l.keys))]

	if !p.Sleep(apiRoundTrip) {
		return false
	}
	obj, err := l.get(key)
	if err != nil {
		return false
	}

	SetReplicas(obj, driftValues[rng.IntN(len(driftValues))])

	if !p.Sleep(apiRoundTrip) {
		return false
	}
	l.applyDriftWrite(obj)
	return true
}

// sweep is the periodic full resync: every key is handed to the requeue set
// again, whether or not anything happened to it.
//
// IT IS WHY STAND-DOWN IS A POLICY RATHER THAN A BUG. A loop that abandons a
// pass on the first refusal only converges because something hands the key back,
// and in a real controller that something is the resync. It is also what makes a
// lost watch event survivable, which is the deeper reason level-triggered
// reconciliation is the model Kubernetes chose.
func (l *Lab) sweep(p Participant) {
	for {
		if !p.Sleep(l.cfg.SweepInterval) {
			return
		}
		if !l.enqueueAll() {
			return
		}
	}
}

// work is one reconcile worker: take a key, reconcile it, repeat. idx is the
// worker's stable index, which is how a waiting worker is chosen — see
// takeParkedLocked.
func (l *Lab) work(p Participant, idx int) {
	for {
		key, ok := l.nextKey(idx)
		if !ok {
			return
		}
		l.reconcile(p, key)
	}
}

// reconcile drives one object toward the target, applying the conflict policy
// for as long as the policy says to keep trying.
//
// EVERY ARM IS CAPPED AT maxAttemptsPerPass, including retry-now, because a real
// controller hands the key back to a rate-limited queue rather than holding a
// worker on one object forever. An uncapped arm would be a strawman.
func (l *Lab) reconcile(p Participant, key string) {
	for attempt := range maxAttemptsPerPass {
		if !p.Sleep(apiRoundTrip) {
			return
		}
		obj, err := l.get(key)
		if err != nil {
			l.noteReadError()
			return
		}
		l.notePass()

		if Replicas(obj) == targetReplicas {
			return
		}
		SetReplicas(obj, targetReplicas)

		if !p.Sleep(apiRoundTrip) {
			return
		}
		if l.applyLoopWrite(obj) != writeConflicted {
			return
		}

		switch l.cfg.Policy {
		case PolicyStandDown:
			// Hand the key back untouched. The next landed write or the next
			// sweep will bring it round again; nothing else is spent here.
			l.noteStoodDown()
			return
		case PolicyRetryBackoff:
			if !p.Sleep(backoffFor(attempt)) {
				return
			}
		case PolicyRetryNow:
			// No added delay. The two round trips above are the whole cost of an
			// attempt, which is what makes this arm fast when it wins and
			// expensive when it does not.
		default:
			// Unreachable through New: Normalize is the only door and it
			// substitutes a valid policy. Returning rather than assuming keeps
			// an unknown policy from silently behaving like one of the three.
			return
		}
	}
	l.noteGaveUp()
}

// --- the store, and the bookkeeping that has to be atomic with it -----------

// get reads one object. It takes the Lab lock for the reason the type doc gives.
func (l *Lab) get(key string) (*appsv1.Deployment, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.store.Get(key)
}

// applyLoopWrite attempts the reconcile loop's compare-and-swap and records what
// happened, in one critical section.
func (l *Lab) applyLoopWrite(obj *appsv1.Deployment) writeOutcome {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.writeAttempts++
	next, err := l.store.Update(obj)
	if err != nil {
		return l.noteWriteFailureLocked(err, &l.conflicts)
	}
	l.writesLanded++
	l.noteWriteLocked(next)
	return writeLanded
}

// applyDriftWrite attempts the competing writer's compare-and-swap and records
// what happened, in one critical section.
func (l *Lab) applyDriftWrite(obj *appsv1.Deployment) writeOutcome {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.driftAttempts++
	next, err := l.store.Update(obj)
	if err != nil {
		return l.noteWriteFailureLocked(err, &l.driftConflicts)
	}
	l.driftLanded++
	l.noteWriteLocked(next)
	return writeLanded
}

// noteWriteFailureLocked classifies a refused write. The conflict counter is a
// parameter because the two writers keep separate tallies — the same mechanism
// seen from each side — while everything else about a refusal is identical.
func (l *Lab) noteWriteFailureLocked(err error, conflicts *uint64) writeOutcome {
	if apierrors.IsConflict(err) {
		(*conflicts)++
		return writeConflicted
	}
	l.rejected++
	return writeRejected
}

// noteWriteLocked records the effect of a LANDED write on the divergence
// bookkeeping, and — for the two edge-triggered policies — hands the key back to
// the requeue set.
//
// THE REQUEUE IS NOT A MISTAKE FOR THOSE TWO. A controller watching its own
// objects is notified of its own writes too, and that self-notification is a
// real source of reconcile passes that find nothing to do — visible on this page
// as ReconcilePasses running well ahead of WriteAttempts. Suppressing it would
// make the loop look cheaper than a real one.
//
// STAND-DOWN IS LEVEL-TRIGGERED AND TAKES NO EVENTS AT ALL, and that is a MODEL
// CORRECTION made in PF-S193c on the evidence of a measurement, not a shortcut.
// The first version enqueued on every landed write regardless of policy, and at
// the most contended point of the sweep stand-down came out BYTE-IDENTICAL to
// retry-now — 1127 attempts, 97 landed, 1030 conflicts, 0.9139 wasted, both.
// The reason is that "give up and wait to be handed the key again" was
// instantaneous: the very write that beat it enqueued the key, so the pass it
// abandoned restarted immediately through the queue instead of through the retry
// branch. It was not standing down, it was retrying by another route, and the
// page would have been showing one policy under two names.
//
// So under stand-down the periodic sweep is the ONLY path back to an object.
// That is not a fudge to force a difference — it is the genuine distinction
// between an edge-triggered controller, which reacts to events, and a
// level-triggered one, which reads the world on a timer and reconciles whatever
// it finds. The two are the deepest choice in the reconcile model, and the demo
// is better for having one of each rather than three of one.
func (l *Lab) noteWriteLocked(obj *appsv1.Deployment) {
	key := obj.Name
	now := l.clock.Now()

	if Replicas(obj) == targetReplicas {
		if at, diverged := l.divergedAt[key]; diverged {
			l.hist.observe(now.Sub(at))
			delete(l.divergedAt, key)
			l.restores++
		}
	} else if _, diverged := l.divergedAt[key]; diverged {
		// A further competing write onto an object that was already wrong. It
		// does not start a new divergence and must not restart its clock: the
		// figure being measured is how long the object was off target.
		l.superseded++
	} else {
		l.divergedAt[key] = now
		l.driftEvents++
	}

	if l.cfg.Policy != PolicyStandDown {
		l.enqueueLocked(key)
	}
}

// --- the requeue set --------------------------------------------------------

// enqueueLocked adds a key to the requeue set. The caller must hold l.mu.
//
// IT DEDUPLICATES, which is what a controller's work queue does: a key already
// waiting is not added twice, so a burst of ten events on one object costs one
// reconcile rather than ten. QueueDepth is therefore a count of distinct objects
// waiting, never of events received.
func (l *Lab) enqueueLocked(key string) {
	if l.queued[key] {
		return
	}
	l.queued[key] = true

	// A WORKER IS WAITING: hand it the key here, in this critical section, and
	// never put the key in the shared queue at all. Signaling and then leaving
	// the key in the queue is what let a different, already-running worker take
	// it — see the parked field.
	if ch := l.takeParkedLocked(); ch != nil {
		ch <- key
		return
	}

	l.order = append(l.order, key)
	if len(l.order) > l.maxQueueDepth {
		l.maxQueueDepth = len(l.order)
	}
}

// enqueueAll hands every key back, which is what the periodic sweep does. It
// reports false once the session has ended.
func (l *Lab) enqueueAll() bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.stopped {
		return false
	}
	for _, k := range l.keys {
		l.enqueueLocked(k)
	}
	return true
}

// nextKey returns the next key for worker `idx`, parking while there is none. It
// reports false only when the session has ended.
//
// The lock is released by hand around the park rather than held across it, which
// is the cost of using a channel per worker instead of a sync.Cond. See the
// parked field for why that cost is worth paying.
func (l *Lab) nextKey(idx int) (string, bool) {
	l.mu.Lock()

	if l.stopped {
		l.mu.Unlock()
		return "", false
	}
	if len(l.order) > 0 {
		k := l.order[0]
		l.order = l.order[1:]
		delete(l.queued, k)
		l.mu.Unlock()
		return k, true
	}

	ch := l.parkLocked(idx)
	l.mu.Unlock()

	// The key arrives already claimed: whoever enqueued it removed it from the
	// shared queue's path entirely, so there is no second worker that could have
	// taken it in between. A closed channel means the session ended.
	k, ok := <-ch
	if !ok {
		return "", false
	}

	l.mu.Lock()
	delete(l.queued, k)
	l.mu.Unlock()
	return k, true
}

// --- small counters ---------------------------------------------------------

func (l *Lab) notePass() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.passes++
}

func (l *Lab) noteStoodDown() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.stoodDown++
}

func (l *Lab) noteGaveUp() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.gaveUp++
}

func (l *Lab) noteReadError() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.readErrors++
}

// --- shutdown ---------------------------------------------------------------

// halt ends the session and releases everyone parked on the requeue set.
func (l *Lab) halt() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.stopped {
		return
	}
	l.stopped = true
	l.wakeAllLocked()
}

// parkLocked marks worker idx as waiting and returns the channel its key will
// arrive on. The caller must hold l.mu, and must release it before receiving.
//
// The channel is buffered so that a waker can hand a key over while holding the
// lock without blocking on a receiver that cannot take the lock yet.
//
// THE BLOCKED COUNT IS INCREMENTED HERE AND DECREMENTED BY THE WAKER, and that
// asymmetry is what keeps virtual time honest. If a waiter decremented it after
// waking, there would be a window — between the hand-off and the waiter re-taking
// the lock — in which every participant still looks blocked to the clock, and
// the clock would advance through an instant at which a goroutine was in fact
// runnable. So the WAKER corrects the count before it releases anyone, while it
// still holds the lock, and the window does not exist.
func (l *Lab) parkLocked(idx int) chan string {
	ch := make(chan string, 1)
	l.parked[idx] = ch
	l.clock.Blocked(1)
	return ch
}

// takeParkedLocked claims the LOWEST-INDEXED parked worker and returns its
// channel, or nil when every worker is busy. The caller must hold l.mu.
//
// LOWEST INDEX RATHER THAN LONGEST WAITING, and the reason is stated carefully
// because it was tested rather than assumed. Index is a property of the WORKER;
// arrival order is a property of the MOMENT, and a selection rule that depends
// on when two goroutines happened to reach the same line is the shape of defect
// this whole arrangement exists to remove.
//
// MEASURED, AND WEAKER THAN THE ARGUMENT SUGGESTS: selecting by arrival order
// instead was run 120 times under -race -shuffle=on and stayed byte-identical
// too. So index is not demonstrably NECESSARY here — under the virtual clock,
// workers appear to park in a determined order as well. It is kept because it
// costs nothing and does not depend on that happening to remain true, and the
// honest summary is "belt and braces", not "the fix".
//
// THE FIX WAS THE HAND-OFF ITSELF. See the parked field: giving the woken worker
// its key, in the critical section that enqueued it, is what removed the
// scheduler from the decision.
func (l *Lab) takeParkedLocked() chan string {
	for i, ch := range l.parked {
		if ch != nil {
			l.parked[i] = nil
			l.clock.Blocked(-1)
			return ch
		}
	}
	return nil
}

// wakeAllLocked releases every parked worker, which is what shutdown needs: they
// are not being handed work, they are being told to stop, and a closed channel
// is how that is said. The caller must hold l.mu.
func (l *Lab) wakeAllLocked() {
	for i, ch := range l.parked {
		if ch == nil {
			continue
		}
		l.parked[i] = nil
		l.clock.Blocked(-1)
		close(ch)
	}
}
