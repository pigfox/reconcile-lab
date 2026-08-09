package reconcilelab

import (
	"testing"
	"time"
)

// defaultConfig is the Config a visitor who changes nothing runs.
func defaultConfig() Config {
	return Config{
		Objects:       DefaultObjects,
		DriftRate:     DefaultDriftRate,
		Workers:       DefaultWorkers,
		SweepInterval: DefaultSweepInterval,
		Policy:        DefaultPolicy,
		Lifetime:      DefaultLifetime,
	}
}

func TestNormalizeLeavesAValidConfigAlone(t *testing.T) {
	in := defaultConfig()

	out, clamps := Normalize(in)
	if len(clamps) != 0 {
		t.Errorf("the shipped default was clamped: %+v", clamps)
	}
	if out.Objects != in.Objects || out.DriftRate != in.DriftRate || out.Workers != in.Workers ||
		out.SweepInterval != in.SweepInterval || out.Policy != in.Policy || out.Lifetime != in.Lifetime {
		t.Errorf("Normalize changed a valid config: %+v -> %+v", in, out)
	}
}

// EVERY CEILING, IN BOTH DIRECTIONS, AND EVERY ONE REPORTED. A ceiling that
// silently changed the simulation would put numbers on screen that are not the
// numbers that ran, which is worse than a refusal.
func TestNormalizeClampsEveryFieldAndSaysSo(t *testing.T) {
	for _, tc := range []struct {
		name  string
		in    Config
		field string
		check func(Config) bool
	}{
		{"objects below", Config{Objects: 0}, fieldObjects, func(c Config) bool { return c.Objects == MinObjects }},
		{"objects above", Config{Objects: MaxObjects + 1}, fieldObjects, func(c Config) bool { return c.Objects == MaxObjects }},
		{"drift below", Config{DriftRate: 0}, fieldDrift, func(c Config) bool { return c.DriftRate == MinDriftRate }},
		{"drift above", Config{DriftRate: MaxDriftRate + 1}, fieldDrift, func(c Config) bool { return c.DriftRate == MaxDriftRate }},
		{"workers below", Config{Workers: 0}, fieldWorkers, func(c Config) bool { return c.Workers == MinWorkers }},
		{"workers above", Config{Workers: MaxWorkers + 1}, fieldWorkers, func(c Config) bool { return c.Workers == MaxWorkers }},
		{"sweep below", Config{SweepInterval: time.Nanosecond}, fieldSweep, func(c Config) bool { return c.SweepInterval == MinSweepInterval }},
		{"sweep above", Config{SweepInterval: MaxSweepInterval + time.Second}, fieldSweep, func(c Config) bool { return c.SweepInterval == MaxSweepInterval }},
		{"policy unknown", Config{Policy: "incinerate"}, fieldPolicy, func(c Config) bool { return c.Policy == PolicyRetryNow }},
		{"lifetime below", Config{Lifetime: time.Nanosecond}, fieldLifetime, func(c Config) bool { return c.Lifetime == MinLifetime }},
		{"lifetime above", Config{Lifetime: MaxLifetime + time.Second}, fieldLifetime, func(c Config) bool { return c.Lifetime == MaxLifetime }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out, clamps := Normalize(tc.in)
			if !tc.check(out) {
				t.Errorf("the value was not clamped: %+v", out)
			}
			var named bool
			for _, c := range clamps {
				if c.Field != tc.field {
					continue
				}
				named = true
				if c.Reason == "" {
					t.Errorf("the clamp on %q carries no reason", c.Field)
				}
				if c.Applied == "" {
					t.Errorf("the clamp on %q does not say what was applied", c.Field)
				}
			}
			if !named {
				t.Errorf("no clamp was reported for %q; got %+v", tc.field, clamps)
			}
		})
	}
}

// A ZERO SEED IS THE ABSENCE OF A REQUEST, not a request for zero, so it is
// substituted without a clamp. A clamp there would tell a visitor their setting
// was overruled when they never made one.
func TestNormalizeSubstitutesAnUnsetSeedQuietly(t *testing.T) {
	out, clamps := Normalize(defaultConfig())
	if out.Seed != defaultSeed {
		t.Errorf("Seed = %d, want the package default %d", out.Seed, defaultSeed)
	}
	if len(clamps) != 0 {
		t.Errorf("substituting an unset seed reported a clamp: %+v", clamps)
	}

	chosen := defaultConfig()
	chosen.Seed = 42
	out, _ = Normalize(chosen)
	if out.Seed != 42 {
		t.Errorf("Seed = %d, want the requested 42", out.Seed)
	}
}

func TestPolicyValidity(t *testing.T) {
	for _, p := range Policies() {
		if !p.Valid() {
			t.Errorf("%q is offered by Policies() but reports itself invalid", p)
		}
	}
	if Policy("").Valid() || Policy("retry").Valid() {
		t.Error("an unknown policy reported itself valid")
	}
	if len(Policies()) != 3 {
		t.Errorf("Policies() has %d entries, want the three arms this demo compares", len(Policies()))
	}
}

func TestReconcileCapacityIsTheUncontendedPassRate(t *testing.T) {
	c := Config{Workers: 2}
	// Two workers, two round trips a pass: 2 / (2 * 2ms) = 500 passes a second.
	want := float64(c.Workers) * float64(time.Second) / float64(passRoundTrips*apiRoundTrip)
	if got := c.ReconcileCapacity(); got != want {
		t.Errorf("ReconcileCapacity = %v, want %v", got, want)
	}
	if got := (Config{Workers: 4}).ReconcileCapacity(); got <= c.ReconcileCapacity() {
		t.Error("doubling the workers did not raise the uncontended pass rate")
	}
}

// THE COUNTER-INTUITIVE CONTROL. At a fixed drift rate a LARGER object set is a
// LESS contended one, because the same writes are spread over more objects. The
// page states this number for that reason, and this pins the direction.
func TestDriftPerObjectFallsAsTheSetGrows(t *testing.T) {
	small := Config{DriftRate: 300, Objects: 4}
	large := Config{DriftRate: 300, Objects: 40}

	if small.DriftPerObject() <= large.DriftPerObject() {
		t.Errorf("per-object drift did not fall as the set grew: %v vs %v",
			small.DriftPerObject(), large.DriftPerObject())
	}
	if got, want := small.DriftPerObject(), 75.0; got != want {
		t.Errorf("DriftPerObject = %v, want %v", got, want)
	}
	if got := (Config{DriftRate: 300, Objects: 0}).DriftPerObject(); got != 0 {
		t.Errorf("DriftPerObject with no objects = %v, want 0 rather than a division by zero", got)
	}
}

func TestBackoffLadderGrowsThenHolds(t *testing.T) {
	if got := backoffFor(0); got != backoffBase {
		t.Errorf("backoffFor(0) = %v, want the base %v", got, backoffBase)
	}
	if got := backoffFor(1); got != 2*backoffBase {
		t.Errorf("backoffFor(1) = %v, want %v", got, 2*backoffBase)
	}
	if got := backoffFor(maxAttemptsPerPass); got > backoffCap {
		t.Errorf("backoffFor(%d) = %v, which is above the cap %v", maxAttemptsPerPass, got, backoffCap)
	}
	if got := backoffFor(backoffMaxShift); got != backoffCap {
		t.Errorf("backoffFor(%d) = %v, want the cap %v", backoffMaxShift, got, backoffCap)
	}
	if got := backoffFor(backoffMaxShift + 1000); got != backoffCap {
		t.Errorf("a very large attempt count gave %v, want the cap %v", got, backoffCap)
	}
}

// A NEGATIVE DELAY WOULD SILENTLY TURN THIS ARM INTO RETRY-NOW, because a Sleep
// of a negative duration returns immediately. The shift is bounded before it is
// taken so that can never happen, and this is the assertion that says so.
func TestBackoffIsNeverNegative(t *testing.T) {
	for _, attempt := range []int{-1, 0, 1, maxAttemptsPerPass, backoffMaxShift, 1 << 30} {
		if got := backoffFor(attempt); got <= 0 {
			t.Errorf("backoffFor(%d) = %v, which is not a delay at all", attempt, got)
		}
	}
}

// --- the contended-default guard ---------------------------------------------

// THE SHIPPED DEFAULT MUST BE CONTENDED, and this is the guard that keeps it so.
//
// WHY IT EXISTS: the pprof-lab capture grid once shipped an artifact labeled
// "overload" that recorded zero stalls, because the configuration behind the
// label did not actually produce the phenomenon the label named. The equivalent
// failure here is a default under which all three policies converge in one pass
// with no conflicts — three identical flat lines, a comparison that is not one.
// Nothing about that failure looks broken from the outside, which is why it has
// to be asserted rather than eyeballed.
//
// IT IS A STATIC ASSERTION ON THE CONSTANTS, deliberately, and it is the reason
// it can live in a config test rather than a simulation one: a later edit to any
// single constant that made the default comfortable reds this immediately, with
// no run required and no flakiness to argue about. The empirical half — that the
// three arms actually land in different places — is in lab_test.go.
func TestShippedDefaultIsContended(t *testing.T) {
	cfg, clamps := Normalize(defaultConfig())
	if len(clamps) != 0 {
		t.Fatalf("the shipped default does not survive its own ceilings: %+v", clamps)
	}

	// A reconcile pass is exposed to a losing race between its READ and its
	// WRITE, which is one round trip and not the two a whole pass costs. The
	// chance the competing writer lands inside that window is roughly the
	// per-object drift rate times the window.
	window := float64(conflictWindow) / float64(time.Second)
	exposure := cfg.DriftPerObject() * window

	if exposure < minDefaultExposure {
		t.Errorf("the default's conflict exposure is %.3f, below the floor %.3f — at this setting the loop "+
			"wins nearly every race and the three policies would render as three identical flat lines. "+
			"objects=%d drift=%d/s (%.1f/s per object), window=%v",
			exposure, minDefaultExposure, cfg.Objects, cfg.DriftRate, cfg.DriftPerObject(), conflictWindow)
	}

	// The loop must nonetheless be able to keep up in aggregate. A default where
	// the writer simply outruns the loop would show three arms all failing, which
	// is a different uninformative picture rather than an interesting one.
	if cfg.ReconcileCapacity() <= float64(cfg.DriftRate) {
		t.Errorf("the loop's uncontended capacity is %.0f passes/s against %d writes/s — the default must be "+
			"contended, not hopeless", cfg.ReconcileCapacity(), cfg.DriftRate)
	}

	// Stand-down cannot restore anything faster than one sweep, so a sweep
	// interval near the session length would render that arm as a flat line at
	// zero and read as a broken demo rather than a slow policy.
	if cfg.SweepInterval*minSweepsPerSession > cfg.Lifetime {
		t.Errorf("the default fits fewer than %d sweeps into a session (sweep=%v, lifetime=%v), so the "+
			"stand-down arm would have almost nothing to show", minSweepsPerSession, cfg.SweepInterval, cfg.Lifetime)
	}
}

// THE GUARD ABOVE MUST BE ABLE TO FAIL. It passes on a clean tree, so it has to
// be shown rejecting a default that is NOT contended — otherwise a rewrite that
// stopped computing the exposure would look identical to a healthy default.
func TestContendedDefaultGuardIsSharp(t *testing.T) {
	// A comfortable configuration: the same drift rate spread over fifty times
	// as many objects, which is the shape that produces flat lines.
	comfortable := Config{Objects: 200, DriftRate: 1}
	window := float64(conflictWindow) / float64(time.Second)

	if exposure := comfortable.DriftPerObject() * window; exposure >= minDefaultExposure {
		t.Errorf("the exposure rule accepts a configuration of %d objects at %d writes/s (exposure %.5f), "+
			"so it would not have caught an uncontended default",
			comfortable.Objects, comfortable.DriftRate, exposure)
	}

	// And it must still accept the shipped default, or it would be rejecting
	// everything and passing for the wrong reason.
	cfg, _ := Normalize(defaultConfig())
	if exposure := cfg.DriftPerObject() * window; exposure < minDefaultExposure {
		t.Errorf("the exposure rule rejects the shipped default (%.3f)", exposure)
	}
}

// minDefaultExposure is the floor on the shipped default's conflict exposure —
// the rough probability that one reconcile pass loses a race. A tenth means
// roughly one pass in ten meets a competing write, which is enough for the three
// arms to separate within a few seconds of a session.
const minDefaultExposure = 0.10

// minSweepsPerSession is how many full resyncs a default session must fit, so
// the stand-down arm has something to show rather than one data point.
const minSweepsPerSession = 10
