package reconcilelab

import "time"

// THE CEILINGS. Every knob a visitor can turn has a hard maximum here, and the
// clamp is applied in THIS package rather than at the HTTP edge.
//
// That placement is the whole point. A ceiling checked in the handler is a
// ceiling one new caller can forget: a second entry point, a test helper, or an
// MCP surface would each have to remember it, and the first one that does not is
// where a visitor spawns eight hundred goroutines. Config values reach the
// simulation only through Normalize, so there is no path that skips them.
//
// A REQUEST ABOVE A CEILING IS CLAMPED, NEVER REFUSED, and the clamp is reported
// back in the stream. Refusing would teach the visitor nothing; clamping silently
// would show them a simulation that is not the one they asked for and let them
// draw a conclusion from it. Naming the change is the only honest option.
const (
	// MinObjects is one. A single object is enough to produce a conflict, and it
	// is the clearest case to read: one writer, one loop, one contested value.
	MinObjects = 1
	// MaxObjects bounds the object set. Every object is a live map entry with a
	// deep copy handed out on each read, and the whole set is swept on every
	// resync, so this is a real memory and work bound rather than a display
	// limit.
	MaxObjects = 200

	// MinDriftRate is one competing write a second — slow enough that the loop
	// wins every race and the conflict count stays at zero, which is a useful
	// thing to be able to see on purpose.
	MinDriftRate = 1
	// MaxDriftRate is the competing writer's ceiling, in writes per second, and
	// it is not a taste: it is exactly what ONE writer can land, one read plus
	// one write at a round trip each. A higher ceiling would let a visitor ask
	// for a rate the writer physically cannot reach, and the page would then
	// display a requested figure the run never achieved. It shipped at 500 for
	// one afternoon and a request for 300 produced 130.
	MaxDriftRate = int(time.Second / writeCost)

	// MinWorkers is one concurrent reconcile, which is what controller-runtime
	// defaults a controller to.
	MinWorkers = 1
	// MaxWorkers is the per-session ceiling on concurrent reconciles. Each is a
	// real goroutine sleeping out simulated round trips, so this is a resource
	// bound.
	MaxWorkers = 8

	// MinSweepInterval is the floor on the periodic full resync. Below this the
	// sweep enqueues faster than a worker can drain, and the requeue set stops
	// meaning anything.
	MinSweepInterval = 100 * time.Millisecond
	// MaxSweepInterval is the ceiling. It is well under MinLifetime on purpose:
	// the stand-down policy cannot restore anything faster than one sweep, so a
	// sweep interval near the session length would render that arm as a flat
	// line at zero and read as a broken demo rather than a slow policy.
	MaxSweepInterval = 30 * time.Second

	// MinLifetime is the shortest session worth opening a stream for.
	MinLifetime = 1 * time.Second
	// MaxLifetime is the hard stop. The server closes the stream at this point
	// and says so in a final frame — an SSE connection that outlives its own
	// simulation is a socket held open for nothing.
	MaxLifetime = 90 * time.Second

	// MaxGlobalWorkers bounds the reconcile goroutines this package will have
	// alive across EVERY session at once. It is the product of the admission
	// limit and the per-session ceiling (four sessions of eight), so a full house
	// is exactly at the bound and a fifth session is refused before it can
	// reserve anything. Enforced by the package budget, not by any handler.
	MaxGlobalWorkers = 32
)

// apiRoundTrip is what one store call costs in simulated time — a read or a
// write, each paid by the goroutine making it.
//
// IT IS LOAD-BEARING IN TWO SEPARATE WAYS and neither is a detail.
//
// FIRST, it is what lets the virtual clock work at all. A retry policy that
// retried with literally no delay would be a goroutine that is neither sleeping
// nor blocked, so VirtualClock could never reach quiescence and simulated time
// would stop dead. Charging every store call a round trip means even the most
// aggressive arm yields.
//
// SECOND, it is what makes a wasted write cost something. Without it, a refused
// write would be free, every arm would converge identically, and the comparison
// this demo is built on would not exist. Two milliseconds is a plausible
// same-cluster round trip and the exact value is not the point — what matters is
// that a write the server refuses is charged the same as one it accepts, which
// is what actually happens.
const apiRoundTrip = 2 * time.Millisecond

// writeCost is what one read-modify-write costs its author in simulated time:
// the read's round trip and the write's. It bounds how fast a single writer can
// possibly go, which is where MaxDriftRate comes from.
const writeCost = passRoundTrips * apiRoundTrip

// conflictWindow is how long a reconcile pass is EXPOSED to losing a race, and
// it is one round trip rather than two.
//
// THE DISTINCTION IS THE WHOLE ARITHMETIC OF THIS DEMO and the contended-default
// guard got it wrong on the first pass. A pass costs two round trips, but the
// object is only held between the READ and the WRITE — the first round trip is
// spent before the object has been read at all, and a competing write landing
// then changes nothing the pass is carrying. Using the pass cost as the window
// overstates the conflict exposure by exactly a factor of two, which is how a
// default that produced a five per cent conflict rate passed a guard that
// believed it had asked for thirty.
const conflictWindow = apiRoundTrip

// The backoff ladder used by the retry-with-backoff policy: base * 2^attempt,
// capped. They are constants rather than controls because the demo varies the
// POLICY, not the tuning of one arm of it — a visitor who could also tune the
// backoff would be comparing four things at once. The page states both figures.
const (
	backoffBase = 5 * time.Millisecond
	backoffCap  = 500 * time.Millisecond
)

// maxAttemptsPerPass bounds how many times one reconcile pass will retry before
// giving up and leaving the key for the next event or sweep.
//
// EVERY ARM IS CAPPED, INCLUDING RETRY-NOW, and that is what keeps the
// comparison honest rather than rigged: an uncapped immediate retry would hold
// one worker on one object for the whole session, and the resulting numbers
// would be measuring a strawman no real controller ships. A real controller
// hands the key back to its rate-limited queue instead, which is what this does.
const maxAttemptsPerPass = 8

// drifterMinTick is the shortest interval the competing writer will sleep for.
// Above MaxDriftRate/1000 writes a second the exact gap is shorter than this, so
// the writer wakes on this tick and emits the whole batch that came due.
//
// WITHOUT IT the ceiling would mean five hundred timer wakeups a second per
// session and two thousand across a full house, which is more scheduler traffic
// than the work being scheduled.
const drifterMinTick = 1 * time.Millisecond

// The competing writer's jitter, as a fraction of the nominal interval either
// side of it.
//
// IT EXISTS TO PREVENT A LOCKSTEP ARTIFACT, not for realism's sake. A writer on
// an exactly periodic schedule racing a loop on an exactly periodic schedule can
// settle into a phase relationship where it wins every race or loses every race,
// and either would be a property of the two periods rather than of the policy
// under test. The draw comes from a seeded generator, so a run is still
// reproducible from its config alone.
const driftJitterFraction = 0.5

// targetReplicas is the replica count the reconcile loop drives every object
// toward — the desired state, held by the controller rather than in the object.
const targetReplicas int32 = 3

// driftValues are the replica counts the competing writer chooses between.
//
// EVERY ONE OF THEM IS OFF TARGET, and a test pins that. If one of these equaled
// targetReplicas, some fraction of competing writes would land in agreement with
// the loop, those writes would silently not count as divergences, and the drift
// rate on screen would overstate the contention actually being created — a
// number that is wrong in the flattering direction, which is the worst kind.
//
// It is a table rather than a computed range so that choosing one is an index
// into a slice. That keeps the whole path free of integer conversions, which
// matters only because the alternative — converting a generator's int down to
// the int32 the real Deployment type wants — is the shape a static analyzer
// flags as a possible overflow, and silencing it would be noise around a value
// that is provably a single digit.
var driftValues = []int32{4, 5, 6, 7, 8}

// unsetReplicas is what Replicas reports for an object carrying a nil
// Spec.Replicas. It is negative so it can never be mistaken for a real count and
// can never equal targetReplicas.
const unsetReplicas int32 = -1

// firstResourceVersion is where the store's version counter starts. It is
// non-zero so that an object carrying the zero value of the field — an empty
// string, or a struct nobody filled in — can never accidentally match a stored
// version and slip past the compare-and-swap.
const firstResourceVersion uint64 = 1000

// The seed object's shape. Nothing in the simulation reads any of it; it is
// here so the object is a valid Deployment rather than a name with a number
// attached. See NewDeployment.
const (
	deploymentNamespace = "default"
	deploymentLabelKey  = "app"
	containerName       = "app"
	containerImage      = "registry.example/app:1"
)

// defaultSeed is the competing writer's random seed when a Config does not carry
// one. It is a constant rather than a clock read so that a session is
// reproducible from its configuration alone — the same controls give the same
// run, which is what makes a screenshot of this page checkable.
const defaultSeed uint64 = 1

// pcgStream is the second word of the generator's state — PCG takes a seed and a
// stream selector, and holding the stream fixed means Config.Seed alone decides
// the sequence. Naming it here keeps that decision out of the call site.
const pcgStream uint64 = 0x9E3779B97F4A7C15

// restoreBoundsMillis are the histogram's fixed bucket upper bounds, in
// milliseconds. There is no external dependency behind them and no dynamic
// resizing: a fixed ladder is what makes a percentile cheap enough to compute on
// every frame, ten times a second, without holding every sample.
//
// The ladder starts at four milliseconds because that is the floor on any
// restore — one read plus one write, each a round trip — and reaches ninety
// seconds because the worst reachable restore is bounded by the session
// lifetime, and a percentile that saturates at its top bucket reports a floor
// rather than a number.
var restoreBoundsMillis = []float64{
	4, 6, 10, 16, 25, 40, 63, 100, 160, 250,
	400, 630, 1000, 1600, 2500, 4000, 6300, 10000, 16000, 25000,
	40000, 63000, 90000,
}

// The quantiles the snapshot reports. Named rather than written at the call site
// so the three the page renders and the three the histogram is asked for cannot
// come apart.
const (
	quantileP50 = 0.50
	quantileP95 = 0.95
	quantileP99 = 0.99
)
