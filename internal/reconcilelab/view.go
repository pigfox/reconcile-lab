package reconcilelab

import "time"

// RepoURL is the simulation's source, read by demoregistry so the repo link has
// one home rather than being restated per surface.
const RepoURL = "https://github.com/pigfox/reconcile-lab"

// The page's asset request paths, each with the cache-busting query this site's
// per-page assets use.
//
// NOT SHELL ASSETS. /js/ and /css/ are served with a 30-day max-age, so the ?v=
// here is the whole cache-busting story: editing one of these files without
// bumping its ?v= ships bytes no returning browser will ever ask for, and
// TestPageAssetVersionsTrackTheirContent fails until both move together.
const (
	// PageJSHref opens the stream, renders the frames and drives the controls.
	PageJSHref = "/js/reconcilelab.js?v=1"
	// PageCSSHref is the page's own chrome.
	PageCSSHref = "/css/reconcile-lab-demo.css?v=1"
)

// StreamPath is the SSE endpoint. It is declared here, beside the page's other
// request paths, so the template and the page script take it from one place
// rather than each spelling a URL.
const StreamPath = "/demos/reconcile-lab/stream"

// FrameInterval is how often a running session emits a metrics frame — ten a
// second. Faster than that and consecutive frames are indistinguishable to a
// reader; slower and a burst of conflicts that opens and closes between frames
// is invisible.
const FrameInterval = 100 * time.Millisecond

// The starting position of the controls.
//
// IT IS DELIBERATELY A CONTENDED ONE, and that is the same decision
// backpressure-lab's overloaded default records. Four objects at three hundred
// competing writes a second is seventy-five writes a second against each object,
// and a reconcile pass is exposed to a losing race for the whole two round trips
// between its read and its write — so a visitor who changes nothing sees
// conflicts, wasted writes, and three policies landing in three different
// places. A comfortable default would show three identical flat lines, which is
// the defect the pprof-lab capture grid shipped once and this line exists to
// prevent.
//
// THE FIGURES HERE ARE PINNED BY TWO TESTS rather than trusted: the static
// contended-default guard in config_test.go, and the empirical three-arm
// comparison in lab_test.go. A later edit to any one of these constants that
// quietly made the default comfortable reds both.
//
// TWO OBJECTS RATHER THAN FOUR OR ONE, measured against the live page: at two,
// the three arms are furthest apart in the figure a reader looks at first —
// refused writes — while every arm still converges most of the time. At one,
// every arm spends most of the session behind and the comparison becomes a race
// between two failures; at four and above the wasted-write ratios start to close.
//
// AN EARLIER VERSION OF THIS COMMENT CLAIMED THE DEFAULT SAT "one click below
// the inversion", and that was withdrawn in production verification. The
// inversion is real under the VIRTUAL clock and is asserted there; on the wall
// clock it does not occur at any reachable setting, because timer overshoot and
// scheduler jitter de-synchronize the workers for free. See sweep_test.go.
const (
	DefaultObjects       = 2
	DefaultDriftRate     = MaxDriftRate
	DefaultWorkers       = 2
	DefaultSweepInterval = 1 * time.Second
	DefaultPolicy        = PolicyRetryNow
	DefaultLifetime      = 45 * time.Second
)

// PolicyOption is one entry in the page's policy control.
type PolicyOption struct {
	Value Policy
	Label string
	// Note is the one line under the control saying what this policy costs. The
	// three notes are the demo's argument in three sentences, so they live beside
	// the values rather than in the template, where a fourth policy could be
	// added without one.
	Note string
}

// View is what the template renders. Every field is a compile-time constant or
// derived from one: the page has no request-dependent state, because the state
// it shows arrives over the stream rather than in the document.
type View struct {
	RepoURL     string
	PageJSHref  string
	PageCSSHref string
	StreamPath  string

	FrameIntervalMillis int

	// The starting positions of the controls.
	DefaultObjects      int
	DefaultDriftRate    int
	DefaultWorkers      int
	DefaultSweepMillis  int
	DefaultPolicy       string
	DefaultLifetimeSecs int

	// The ceilings, carried onto the page as data attributes so the controls
	// enforce the SAME numbers the server clamps to and the script has no limit
	// literal of its own. The browser's copy is a convenience; the server's is
	// the rule, and the page says so.
	MinObjects       int
	MaxObjects       int
	MinDriftRate     int
	MaxDriftRate     int
	MinWorkers       int
	MaxWorkers       int
	MinSweepMillis   int
	MaxSweepMillis   int
	MaxLifetimeSecs  int
	MaxGlobalWorkers int
	MaxSessions      int

	// The fixed parameters the page states because a figure on screen is not
	// interpretable without them. The round trip is what a wasted write costs;
	// the backoff pair is the ladder one arm climbs; the attempt cap is why no
	// arm can spend a session on one object; the target is what "converged"
	// means.
	RoundTripMillis   int
	BackoffBaseMillis int
	BackoffCapMillis  int
	MaxAttempts       int
	TargetReplicas    int

	Policies []PolicyOption
}

// NewView builds the only state this page has. maxSessions is the transport's
// admission limit, which lives with the transport rather than here — this
// package bounds reconcile workers, not connections.
func NewView(maxSessions int) View {
	return View{
		RepoURL:     RepoURL,
		PageJSHref:  PageJSHref,
		PageCSSHref: PageCSSHref,
		StreamPath:  StreamPath,

		FrameIntervalMillis: int(FrameInterval / time.Millisecond),

		DefaultObjects:      DefaultObjects,
		DefaultDriftRate:    DefaultDriftRate,
		DefaultWorkers:      DefaultWorkers,
		DefaultSweepMillis:  int(DefaultSweepInterval / time.Millisecond),
		DefaultPolicy:       string(DefaultPolicy),
		DefaultLifetimeSecs: int(DefaultLifetime / time.Second),

		MinObjects:       MinObjects,
		MaxObjects:       MaxObjects,
		MinDriftRate:     MinDriftRate,
		MaxDriftRate:     MaxDriftRate,
		MinWorkers:       MinWorkers,
		MaxWorkers:       MaxWorkers,
		MinSweepMillis:   int(MinSweepInterval / time.Millisecond),
		MaxSweepMillis:   int(MaxSweepInterval / time.Millisecond),
		MaxLifetimeSecs:  int(MaxLifetime / time.Second),
		MaxGlobalWorkers: MaxGlobalWorkers,
		MaxSessions:      maxSessions,

		RoundTripMillis:   int(apiRoundTrip / time.Millisecond),
		BackoffBaseMillis: int(backoffBase / time.Millisecond),
		BackoffCapMillis:  int(backoffCap / time.Millisecond),
		MaxAttempts:       maxAttemptsPerPass,
		TargetReplicas:    int(targetReplicas),

		Policies: []PolicyOption{
			{
				Value: PolicyRetryNow,
				Label: "Retry at once",
				Note:  "Re-read and write again with no delay. Fastest back on target when it wins the next race, and the most refused writes when it does not.",
			},
			{
				Value: PolicyRetryBackoff,
				Label: "Retry with backoff",
				Note:  "Wait a little longer after each refusal. Conceding the object is what lets the other writer finish, so fewer attempts are spent to get the same result.",
			},
			{
				Value: PolicyStandDown,
				Label: "Stand down",
				Note:  "Give up on the first refusal and take no write events at all — the periodic sweep is the only way back to an object. Almost nothing is wasted, and nothing is restored faster than one sweep.",
			},
		},
	}
}
