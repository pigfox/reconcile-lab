package reconcilelab

import (
	"strings"
	"testing"
	"time"
)

// THE PAGE'S NUMBERS ARE THE PACKAGE'S CONSTANTS, not a second copy of them. The
// same contract the backpressure page's guard pins for its ceilings, and for the
// same reason: a limit written twice drifts, and the copy nobody re-reads is the
// one that ends up wrong.
func TestNewViewCarriesTheCeilingsItself(t *testing.T) {
	const maxSessions = 4
	v := NewView(maxSessions)

	for _, tc := range []struct {
		what      string
		got, want int
	}{
		{"MinObjects", v.MinObjects, MinObjects},
		{"MaxObjects", v.MaxObjects, MaxObjects},
		{"MinDriftRate", v.MinDriftRate, MinDriftRate},
		{"MaxDriftRate", v.MaxDriftRate, MaxDriftRate},
		{"MinWorkers", v.MinWorkers, MinWorkers},
		{"MaxWorkers", v.MaxWorkers, MaxWorkers},
		{"MinSweepMillis", v.MinSweepMillis, int(MinSweepInterval / time.Millisecond)},
		{"MaxSweepMillis", v.MaxSweepMillis, int(MaxSweepInterval / time.Millisecond)},
		{"MaxLifetimeSecs", v.MaxLifetimeSecs, int(MaxLifetime / time.Second)},
		{"MaxGlobalWorkers", v.MaxGlobalWorkers, MaxGlobalWorkers},
		{"MaxSessions", v.MaxSessions, maxSessions},
		{"FrameIntervalMillis", v.FrameIntervalMillis, int(FrameInterval / time.Millisecond)},
	} {
		if tc.got != tc.want {
			t.Errorf("%s = %d, want %d", tc.what, tc.got, tc.want)
		}
	}
}

// THE FIXED PARAMETERS REACH THE PAGE TOO, because a figure on screen is not
// interpretable without them. A wasted-write count means nothing until a reader
// knows what a write costs, and a restore latency means nothing until they know
// the loop is bounded to a handful of attempts.
func TestNewViewCarriesTheParametersAFigureNeeds(t *testing.T) {
	v := NewView(1)

	if v.RoundTripMillis != int(apiRoundTrip/time.Millisecond) {
		t.Errorf("RoundTripMillis = %d, want %d", v.RoundTripMillis, int(apiRoundTrip/time.Millisecond))
	}
	if v.BackoffBaseMillis != int(backoffBase/time.Millisecond) {
		t.Errorf("BackoffBaseMillis = %d, want %d", v.BackoffBaseMillis, int(backoffBase/time.Millisecond))
	}
	if v.BackoffCapMillis != int(backoffCap/time.Millisecond) {
		t.Errorf("BackoffCapMillis = %d, want %d", v.BackoffCapMillis, int(backoffCap/time.Millisecond))
	}
	if v.MaxAttempts != maxAttemptsPerPass {
		t.Errorf("MaxAttempts = %d, want %d", v.MaxAttempts, maxAttemptsPerPass)
	}
	if v.TargetReplicas != int(targetReplicas) {
		t.Errorf("TargetReplicas = %d, want %d", v.TargetReplicas, targetReplicas)
	}
}

func TestNewViewCarriesTheShippedDefaults(t *testing.T) {
	v := NewView(1)

	if v.DefaultObjects != DefaultObjects || v.DefaultDriftRate != DefaultDriftRate ||
		v.DefaultWorkers != DefaultWorkers || v.DefaultPolicy != string(DefaultPolicy) {
		t.Errorf("the view's defaults are not the package's: %+v", v)
	}
	if v.DefaultSweepMillis != int(DefaultSweepInterval/time.Millisecond) {
		t.Errorf("DefaultSweepMillis = %d, want %d", v.DefaultSweepMillis, int(DefaultSweepInterval/time.Millisecond))
	}
	if v.DefaultLifetimeSecs != int(DefaultLifetime/time.Second) {
		t.Errorf("DefaultLifetimeSecs = %d, want %d", v.DefaultLifetimeSecs, int(DefaultLifetime/time.Second))
	}
}

// EVERY DEFAULT MUST BE ENTERABLE. The page's number inputs are built from the
// minimum, the maximum and a step, and a default outside that ladder is a value
// the browser refuses — which is exactly the defect backpressure-lab shipped,
// where the field's own recommended value could not be typed into it.
func TestEveryDefaultIsInsideItsOwnBounds(t *testing.T) {
	v := NewView(1)

	// The bounds are named lo/hi rather than min/max: those two are predeclared
	// identifiers in Go, and shadowing a builtin in a test fixture is the kind of
	// thing a linter is right to object to.
	for _, tc := range []struct {
		what        string
		lo, val, hi int
	}{
		{"objects", v.MinObjects, v.DefaultObjects, v.MaxObjects},
		{"drift rate", v.MinDriftRate, v.DefaultDriftRate, v.MaxDriftRate},
		{"workers", v.MinWorkers, v.DefaultWorkers, v.MaxWorkers},
		{"sweep", v.MinSweepMillis, v.DefaultSweepMillis, v.MaxSweepMillis},
	} {
		if tc.val < tc.lo || tc.val > tc.hi {
			t.Errorf("%s: the shipped default %d is outside [%d, %d]", tc.what, tc.val, tc.lo, tc.hi)
		}
	}
	if v.DefaultLifetimeSecs > v.MaxLifetimeSecs {
		t.Errorf("the default lifetime %ds is above the ceiling %ds", v.DefaultLifetimeSecs, v.MaxLifetimeSecs)
	}
}

// EVERY POLICY IS OFFERED, WITH ITS OWN NOTE. The three notes are the demo's
// argument in three sentences, so they live beside the values rather than in the
// template — where a fourth policy could be added without one.
func TestEveryPolicyIsOfferedWithItsCost(t *testing.T) {
	v := NewView(1)

	if len(v.Policies) != len(Policies()) {
		t.Fatalf("the view offers %d policies, want %d", len(v.Policies), len(Policies()))
	}
	for i, want := range Policies() {
		got := v.Policies[i]
		if got.Value != want {
			t.Errorf("policy %d is %q, want %q — the order the page offers them is meaningful", i, got.Value, want)
		}
		if strings.TrimSpace(got.Label) == "" {
			t.Errorf("%q has no label", got.Value)
		}
		if strings.TrimSpace(got.Note) == "" {
			t.Errorf("%q has no note saying what it costs", got.Value)
		}
	}

	seen := map[string]bool{}
	for _, p := range v.Policies {
		if seen[p.Note] {
			t.Errorf("two policies share the note %q, so the control says the same thing twice", p.Note)
		}
		seen[p.Note] = true
	}
}

func TestNewViewCarriesItsAssetPaths(t *testing.T) {
	v := NewView(1)

	if v.RepoURL != RepoURL || v.StreamPath != StreamPath {
		t.Errorf("the view does not carry the package's paths: %+v", v)
	}
	for _, href := range []string{v.PageJSHref, v.PageCSSHref} {
		if !strings.Contains(href, "?v=") {
			t.Errorf("%q carries no cache-busting query — /js/ and /css/ are served with a 30-day max-age, "+
				"so editing the file without bumping this ships bytes no returning browser asks for", href)
		}
	}
	if !strings.HasPrefix(v.StreamPath, "/demos/") {
		t.Errorf("StreamPath = %q, want it under /demos/", v.StreamPath)
	}
}
