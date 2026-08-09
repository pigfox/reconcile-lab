package reconcilelab

import (
	"testing"
	"time"
)

// armResult is one policy's outcome at one contention setting.
type armResult struct {
	policy Policy
	snap   Snapshot
}

// sweepAt runs all three arms at one setting and returns their snapshots.
func sweepAt(t *testing.T, objects, drift, workers int, sweep time.Duration) []armResult {
	t.Helper()

	out := make([]armResult, 0, len(Policies()))
	for _, p := range Policies() {
		cfg := Config{
			Objects:       objects,
			DriftRate:     drift,
			Workers:       workers,
			SweepInterval: sweep,
			Policy:        p,
			Lifetime:      measuredLifetime,
		}
		snap, reason := runVirtual(t, cfg)
		if reason != StopLifetime {
			t.Fatalf("objects=%d drift=%d workers=%d %s: ended as %q", objects, drift, workers, p, reason)
		}
		out = append(out, armResult{policy: p, snap: snap})
	}
	return out
}

// THE CONTENTION SWEEP. It is a test rather than a one-off script because the
// question it answers — where the retry-now/backoff crossing sits, and whether
// there is one — is a claim the page makes, and a claim the page makes has to be
// re-derivable on demand rather than remembered from an afternoon.
//
// It is run with -v to read the table; the assertions below it are what a
// routine run enforces.
func TestContentionSweep(t *testing.T) {
	t.Logf("%-8s %-6s %-4s | %-14s %8s %8s %8s %8s %8s %8s %6s",
		"objects", "drift", "wrk", "policy", "achieved", "attempts", "landed", "confl", "wasted", "p95", "div")

	type point struct {
		exposure       float64
		nowWasted      float64
		backoffWasted  float64
		standWasted    float64
		nowP95         time.Duration
		backoffP95     time.Duration
		standP95       time.Duration
		nowRestores    uint64
		standStoodDown uint64
	}
	points := make([]point, 0, len(sweepSettings))

	for _, s := range sweepSettings {
		arms := sweepAt(t, s.objects, s.drift, s.workers, s.sweep)
		by := map[Policy]Snapshot{}
		for _, a := range arms {
			by[a.policy] = a.snap
			t.Logf("%-8d %-6d %-4d | %-14s %8.1f %8d %8d %8d %8.4f %8v %6d",
				s.objects, s.drift, s.workers, a.policy, a.snap.DriftRateAchieved,
				a.snap.WriteAttempts, a.snap.WritesLanded, a.snap.Conflicts,
				a.snap.WastedWriteRatio, a.snap.P95, a.snap.Diverged)
		}
		cfg, _ := Normalize(Config{Objects: s.objects, DriftRate: s.drift, Workers: s.workers,
			SweepInterval: s.sweep, Policy: PolicyRetryNow, Lifetime: measuredLifetime})
		points = append(points, point{
			exposure:       cfg.DriftPerObject() * float64(conflictWindow) / float64(time.Second),
			nowWasted:      by[PolicyRetryNow].WastedWriteRatio,
			backoffWasted:  by[PolicyRetryBackoff].WastedWriteRatio,
			standWasted:    by[PolicyStandDown].WastedWriteRatio,
			nowP95:         by[PolicyRetryNow].P95,
			backoffP95:     by[PolicyRetryBackoff].P95,
			standP95:       by[PolicyStandDown].P95,
			nowRestores:    by[PolicyRetryNow].Restores,
			standStoodDown: by[PolicyStandDown].StoodDown,
		})
	}

	// WASTED WRITES ARE MONOTONE BY CONSTRUCTION at every setting: one arm keeps
	// trying, one waits between tries, one stops after the first refusal. If this
	// ever failed, the arms would not be doing what their names say.
	for i, p := range points {
		if p.nowWasted < p.backoffWasted {
			t.Errorf("setting %d: retry-now wasted %.4f and backoff %.4f — the unthrottled arm must waste more",
				i, p.nowWasted, p.backoffWasted)
		}
		if p.backoffWasted < p.standWasted {
			t.Errorf("setting %d: backoff wasted %.4f and stand-down %.4f — the arm that retries must waste "+
				"more than the arm that does not", i, p.backoffWasted, p.standWasted)
		}
	}

	// AND CONTENTION HAS TO RISE ACROSS THE SWEEP, or the sweep is not one.
	if points[0].exposure >= points[len(points)-1].exposure {
		t.Errorf("the sweep does not increase contention: %.4f at the start, %.4f at the end",
			points[0].exposure, points[len(points)-1].exposure)
	}

	// THE SEPARATION MUST WIDEN WITH CONTENTION. That is the shape of the claim:
	// at low contention the arms are nearly indistinguishable and the choice does
	// not matter; the reason to care is what happens when contention rises.
	lowSpread := points[0].nowWasted - points[0].standWasted
	highSpread := points[len(points)-1].nowWasted - points[len(points)-1].standWasted
	t.Logf("wasted-write spread: %.4f at exposure %.3f, %.4f at exposure %.3f",
		lowSpread, points[0].exposure, highSpread, points[len(points)-1].exposure)
	if highSpread <= lowSpread {
		t.Errorf("the arms did not separate further as contention rose: spread %.4f -> %.4f", lowSpread, highSpread)
	}

	// THE INVERSION — AND IT IS A PROPERTY OF THIS MODEL UNDER EXACT TIMING,
	// NOT A PREDICTION ABOUT THE LIVE PAGE. At low contention retrying at once
	// restores faster than backing off. At the top of the sweep it REVERSES,
	// because concurrent reconciles running to the microsecond settle into a
	// rhythm where they collide with each other as well as with the competing
	// writer, and waiting is what breaks that rhythm.
	//
	// IT DOES NOT REPRODUCE ON THE WALL CLOCK, and that was found in production
	// verification rather than here. Seven runs against the live page put
	// retry-now's p95 BELOW backoff's at every reachable setting — 25 ms against
	// 40 ms at one object, 16 ms against 25 ms at two — because real timers
	// overshoot and the scheduler jitters, which de-synchronizes the workers for
	// free and does what backing off pays for.
	//
	// The card copy claimed the swap and was corrected; this assertion stays,
	// scoped to what it actually covers. Both statements are true of different
	// clocks, and the page now says which one its figures came from.
	low, high := points[0], points[len(points)-1]
	t.Logf("p95 restore: at exposure %.3f retry-now=%v backoff=%v; at exposure %.3f retry-now=%v backoff=%v",
		low.exposure, low.nowP95, low.backoffP95, high.exposure, high.nowP95, high.backoffP95)

	if low.nowP95 > low.backoffP95 {
		t.Errorf("at the LEAST contended setting (exposure %.3f) retry-now's p95 is %v against backoff's %v — "+
			"the unthrottled arm is meant to be the faster one where races are rare",
			low.exposure, low.nowP95, low.backoffP95)
	}
	if high.nowP95 <= high.backoffP95 {
		t.Errorf("at the MOST contended setting (exposure %.3f) retry-now's p95 is %v against backoff's %v — "+
			"the inversion the page claims did not happen, and the claim has to change rather than the assertion",
			high.exposure, high.nowP95, high.backoffP95)
	}

	// AND THE CROSSING HAS TO BE SOMEWHERE IN BETWEEN, at a setting a visitor can
	// actually reach. Reporting where it sits is the finding; a crossing that
	// existed only outside the controls' range would be a claim nobody could check.
	crossed := -1
	for i, p := range points {
		if p.nowP95 > p.backoffP95 {
			crossed = i
			break
		}
	}
	if crossed <= 0 {
		t.Errorf("no crossing was found inside the sweep (index %d)", crossed)
	} else {
		t.Logf("the crossing sits between exposure %.3f and %.3f — %d object(s) against %d, at %d writes/s",
			points[crossed-1].exposure, points[crossed].exposure,
			sweepSettings[crossed].objects, sweepSettings[crossed-1].objects, sweepSettings[crossed].drift)
	}
}

// sweepSettings are the contention points, ordered from least to most contended.
// Objects falls while the drift rate holds at the writer's ceiling, so the
// per-object rate — which is what actually decides a race — climbs.
var sweepSettings = []struct {
	objects, drift, workers int
	sweep                   time.Duration
}{
	{objects: 32, drift: MaxDriftRate, workers: 2, sweep: time.Second},
	{objects: 16, drift: MaxDriftRate, workers: 2, sweep: time.Second},
	{objects: 8, drift: MaxDriftRate, workers: 2, sweep: time.Second},
	{objects: 4, drift: MaxDriftRate, workers: 2, sweep: time.Second},
	{objects: 2, drift: MaxDriftRate, workers: 2, sweep: time.Second},
	{objects: 1, drift: MaxDriftRate, workers: 2, sweep: time.Second},
}
