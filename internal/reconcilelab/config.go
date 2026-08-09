package reconcilelab

import (
	"strconv"
	"time"
)

// Policy is what the reconcile loop does when the store refuses its write. The
// three values are the whole argument of this demo: the same contention, three
// answers, and each pays in a different currency.
type Policy string

const (
	// PolicyRetryNow re-reads and writes again with no added delay. It still
	// pays the round trips — see apiRoundTrip — so it is not free, it is merely
	// unthrottled. It restores fastest when it wins and wastes the most writes
	// when it does not.
	PolicyRetryNow Policy = "retry-now"

	// PolicyRetryBackoff re-reads and writes again after a delay that doubles
	// with each attempt, capped. It concedes the object for a moment, which is
	// exactly what lets the competing writer finish and the next attempt land.
	PolicyRetryBackoff Policy = "retry-backoff"

	// PolicyStandDown abandons the pass on the first refusal, and takes no write
	// events at all: the periodic sweep is its only path back to an object.
	//
	// IT IS THE LEVEL-TRIGGERED ARM, and that is the real distinction rather
	// than a difference of degree. The other two react to events; this one reads
	// the world on a timer and reconciles whatever it finds. It wastes almost
	// nothing and cannot restore anything faster than one sweep, so it is the
	// cheapest arm and the slowest one, in every regime. See noteWriteLocked for
	// the measurement that forced this to be modeled properly.
	PolicyStandDown Policy = "stand-down"
)

// Valid reports whether p is one of the three policies.
func (p Policy) Valid() bool {
	switch p {
	case PolicyRetryNow, PolicyRetryBackoff, PolicyStandDown:
		return true
	default:
		return false
	}
}

// Policies is every policy in the order the page offers them.
func Policies() []Policy { return []Policy{PolicyRetryNow, PolicyRetryBackoff, PolicyStandDown} }

// Config is one session's requested simulation. Values arrive from a query
// string, so nothing here is trusted: Normalize is the only way to build a
// Config the Lab will accept.
type Config struct {
	// Objects is how many Deployments the loop is responsible for. They share
	// one competing writer and one requeue set.
	Objects int

	// DriftRate is the competing writer's rate, in landed-or-refused write
	// attempts per second across the whole object set.
	DriftRate int

	// Workers is how many reconciles run concurrently — the knob
	// controller-runtime spells MaxConcurrentReconciles.
	Workers int

	// SweepInterval is the periodic full resync: every key is handed to the
	// requeue set again, whether or not anything happened to it. It is what the
	// stand-down policy depends on and what makes a lost event survivable.
	SweepInterval time.Duration

	// Policy is what the loop does about a refused write.
	Policy Policy

	// Lifetime is how long the session runs before the server closes it.
	Lifetime time.Duration

	// Seed drives the competing writer's jitter and its choice of object and
	// value. A ZERO VALUE MEANS UNSPECIFIED rather than a request for seed zero,
	// and Normalize substitutes the package default — which is why that
	// substitution is not reported as a clamp: nobody asked for it.
	Seed uint64
}

// Clamp is one ceiling having been applied. It is carried into the stream rather
// than logged, because a visitor who asked for something they did not get needs
// to be told: a simulation quietly running different numbers from the ones on
// screen is worse than a refusal.
type Clamp struct {
	// Field is the control's name as the page spells it.
	Field string
	// Requested is what arrived, formatted the way the field is displayed.
	Requested string
	// Applied is what the simulation will actually use.
	Applied string
	// Reason is one sentence a reader can act on.
	Reason string
}

// The clamp field names, as the page spells them. They are constants because
// the handler's guard asserts on them and a near-miss spelling would be a clamp
// nobody could match.
const (
	fieldObjects  = "objects"
	fieldDrift    = "drift rate"
	fieldWorkers  = "workers"
	fieldSweep    = "sweep interval"
	fieldPolicy   = "policy"
	fieldLifetime = "lifetime"
)

// Normalize clamps a requested Config into the range this package will run, and
// reports every change it made.
//
// It is a PURE FUNCTION and it is the only door into the simulation: New calls
// it on whatever it is given, so a caller cannot construct a Config by hand and
// skip the ceilings. That is the placement the package doc argues for — a limit
// enforced at the HTTP edge is a limit the second entry point forgets.
func Normalize(req Config) (Config, []Clamp) {
	out := req
	var clamps []Clamp

	note := func(field, requested, applied, reason string) {
		clamps = append(clamps, Clamp{Field: field, Requested: requested, Applied: applied, Reason: reason})
	}

	if out.Objects < MinObjects {
		note(fieldObjects, strconv.Itoa(req.Objects), strconv.Itoa(MinObjects),
			"a run with no objects has nothing to contend over and every policy would look the same")
		out.Objects = MinObjects
	}
	if out.Objects > MaxObjects {
		note(fieldObjects, strconv.Itoa(req.Objects), strconv.Itoa(MaxObjects),
			"every object is swept on each resync and copied on each read, so the set is capped per session")
		out.Objects = MaxObjects
	}

	if out.DriftRate < MinDriftRate {
		note(fieldDrift, strconv.Itoa(req.DriftRate)+"/s", strconv.Itoa(MinDriftRate)+"/s",
			"a writer that never writes produces no contention and there is nothing to watch")
		out.DriftRate = MinDriftRate
	}
	if out.DriftRate > MaxDriftRate {
		note(fieldDrift, strconv.Itoa(req.DriftRate)+"/s", strconv.Itoa(MaxDriftRate)+"/s",
			"above this the writer's own loop costs more than the contention it creates")
		out.DriftRate = MaxDriftRate
	}

	if out.Workers < MinWorkers {
		note(fieldWorkers, strconv.Itoa(req.Workers), strconv.Itoa(MinWorkers),
			"a loop with no workers never reconciles anything, so nothing would ever be restored")
		out.Workers = MinWorkers
	}
	if out.Workers > MaxWorkers {
		note(fieldWorkers, strconv.Itoa(req.Workers), strconv.Itoa(MaxWorkers),
			"each concurrent reconcile is a real goroutine, so the pool is capped per session")
		out.Workers = MaxWorkers
	}

	if out.SweepInterval < MinSweepInterval {
		note(fieldSweep, req.SweepInterval.String(), MinSweepInterval.String(),
			"a faster sweep enqueues more than a worker can drain and the requeue set stops meaning anything")
		out.SweepInterval = MinSweepInterval
	}
	if out.SweepInterval > MaxSweepInterval {
		note(fieldSweep, req.SweepInterval.String(), MaxSweepInterval.String(),
			"stand-down cannot restore faster than one sweep, so a longer one would render that arm as a flat line")
		out.SweepInterval = MaxSweepInterval
	}

	if !out.Policy.Valid() {
		note(fieldPolicy, string(req.Policy), string(PolicyRetryNow),
			"the conflict policy must be one of the three offered")
		out.Policy = PolicyRetryNow
	}

	if out.Lifetime < MinLifetime {
		note(fieldLifetime, req.Lifetime.String(), MinLifetime.String(),
			"a shorter session ends before the loop has reconciled anything")
		out.Lifetime = MinLifetime
	}
	if out.Lifetime > MaxLifetime {
		note(fieldLifetime, req.Lifetime.String(), MaxLifetime.String(),
			"the server closes the stream at this point and says so in a final frame")
		out.Lifetime = MaxLifetime
	}

	// Not a clamp. See Config.Seed: zero is the absence of a request.
	if out.Seed == 0 {
		out.Seed = defaultSeed
	}

	return out, clamps
}

// ReconcileCapacity is how many reconcile passes a second this loop could run if
// nothing ever conflicted — one read and one write per pass, each a round trip,
// across the whole worker pool.
//
// The page shows it beside the drift rate so the contention point is a stated
// number rather than something a visitor has to infer from a chart. It is the
// same role Capacity plays on the backpressure page, and the same caveat
// applies: it is a ceiling the run will not reach, because a refused write costs
// a pass and buys nothing.
func (c Config) ReconcileCapacity() float64 {
	return float64(c.Workers) * float64(time.Second) / float64(passRoundTrips*apiRoundTrip)
}

// DriftPerObject is the competing writer's rate against any ONE object, which is
// the figure that actually decides how often the loop loses a race. The rate
// across the whole set is spread over the objects, so at a fixed drift rate a
// larger object set is a LESS contended one — the single most counter-intuitive
// thing about the controls, and why the page states this number too.
func (c Config) DriftPerObject() float64 {
	if c.Objects <= 0 {
		return 0
	}
	return float64(c.DriftRate) / float64(c.Objects)
}

// passRoundTrips is how many store calls one uncontended reconcile pass makes:
// the read, and the write.
const passRoundTrips = 2

// backoffFor is the delay before attempt n, counted from zero. It doubles and
// then holds at the cap.
//
// The shift is bounded before it is taken. maxAttemptsPerPass keeps n small in
// practice, but a shift wide enough to overflow the duration would produce a
// NEGATIVE delay, and a negative delay is a Sleep that returns immediately —
// which would silently turn this arm into retry-now.
func backoffFor(attempt int) time.Duration {
	if attempt < 0 {
		return backoffBase
	}
	if attempt >= backoffMaxShift {
		return backoffCap
	}
	d := backoffBase << uint(attempt)
	if d > backoffCap {
		return backoffCap
	}
	return d
}

// backoffMaxShift is where doubling stops being computed and starts being
// answered with the cap. It is small enough that the shift above can never
// approach the width of a Duration.
const backoffMaxShift = 20
