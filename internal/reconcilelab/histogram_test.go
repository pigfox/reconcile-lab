package reconcilelab

import (
	"testing"
	"time"
)

// A PERCENTILE OVER NO RESTORES IS NOT A SMALL NUMBER, IT IS THE ABSENCE OF ONE.
// The page renders zero as a dash for exactly this reason, and a histogram that
// returned a plausible-looking small duration instead would be inventing data.
func TestHistogramReportsNothingBeforeAnyRestore(t *testing.T) {
	h := newHistogram()

	if got := h.quantile(quantileP50); got != 0 {
		t.Errorf("p50 over no samples = %v, want 0", got)
	}
	if h.max != 0 {
		t.Errorf("max over no samples = %v, want 0", h.max)
	}
}

// A BUCKETED PERCENTILE RETURNS THE UPPER BOUND OF ITS BUCKET, so it never
// understates. Reporting one as though it were exact is the usual way this kind
// of number misleads.
func TestHistogramNeverUnderstates(t *testing.T) {
	h := newHistogram()
	for range 100 {
		h.observe(5 * time.Millisecond)
	}

	got := h.quantile(quantileP50)
	if got < 5*time.Millisecond {
		t.Errorf("p50 = %v for a sample of 5ms, which understates it", got)
	}
	if got != 6*time.Millisecond {
		t.Errorf("p50 = %v, want the 6ms bucket bound that 5ms falls in", got)
	}
}

// The split is 90/10 rather than 99/1 on purpose. With a single slow sample out
// of a hundred, rank 99 is still a fast one and the p99 is CORRECTLY the fast
// bucket — so a 99/1 fixture would assert something false about the histogram
// rather than something true about the tail.
func TestHistogramSeparatesTheTail(t *testing.T) {
	h := newHistogram()
	for range 90 {
		h.observe(5 * time.Millisecond)
	}
	for range 10 {
		h.observe(4 * time.Second)
	}

	p50 := h.quantile(quantileP50)
	p99 := h.quantile(quantileP99)
	if p99 <= p50 {
		t.Errorf("p99 (%v) is not above p50 (%v); the tail sample vanished", p99, p50)
	}
	if h.max != 4*time.Second {
		t.Errorf("max = %v, want 4s", h.max)
	}
}

// THE OVERFLOW BUCKET HAS NO UPPER BOUND TO REPORT, so the observed maximum is
// returned instead — which keeps the same promise, since the true value cannot
// exceed it.
func TestHistogramOverflowReportsTheObservedMaximum(t *testing.T) {
	h := newHistogram()
	huge := time.Duration(restoreBoundsMillis[len(restoreBoundsMillis)-1]*float64(time.Millisecond)) * 10
	h.observe(huge)

	if got := h.quantile(quantileP99); got != huge {
		t.Errorf("p99 of an overflowing sample = %v, want the observed maximum %v", got, huge)
	}
}

func TestHistogramClampsTheQuantileItIsAsked(t *testing.T) {
	h := newHistogram()
	h.observe(10 * time.Millisecond)

	below := h.quantile(-1)
	above := h.quantile(2)
	if below == 0 {
		t.Error("a quantile below zero returned nothing rather than the smallest sample")
	}
	if above == 0 {
		t.Error("a quantile above one returned nothing rather than the largest sample")
	}
}

// A NEGATIVE DURATION IS RECORDED AS ZERO RATHER THAN DROPPED. It cannot arise
// from a real restore — the clock is monotonic within a run — but silently
// discarding a sample would make the total disagree with the restore count.
func TestHistogramFoldsANegativeSampleToZero(t *testing.T) {
	h := newHistogram()
	h.observe(-time.Second)

	if h.total != 1 {
		t.Errorf("total = %d after one negative observation, want 1", h.total)
	}
	if h.max != 0 {
		t.Errorf("max = %v after a negative observation, want 0", h.max)
	}
	if got := h.quantile(quantileP50); got != time.Duration(restoreBoundsMillis[0]*float64(time.Millisecond)) {
		t.Errorf("p50 = %v, want the first bucket bound", got)
	}
}

// THE LADDER STARTS AT THE FLOOR ON ANY RESTORE. A restore costs at least one
// read and one write, so a bucket below that could never be occupied and would
// only make the first percentile look precise.
func TestRestoreLadderStartsAtTheCheapestPossibleRestore(t *testing.T) {
	floor := float64(passRoundTrips*apiRoundTrip) / float64(time.Millisecond)
	if restoreBoundsMillis[0] < floor {
		t.Errorf("the first bucket bound is %vms, below the %vms floor on any restore",
			restoreBoundsMillis[0], floor)
	}
	for i := 1; i < len(restoreBoundsMillis); i++ {
		if restoreBoundsMillis[i] <= restoreBoundsMillis[i-1] {
			t.Fatalf("the ladder is not increasing at index %d: %v then %v",
				i, restoreBoundsMillis[i-1], restoreBoundsMillis[i])
		}
	}
	top := restoreBoundsMillis[len(restoreBoundsMillis)-1]
	if want := float64(MaxLifetime / time.Millisecond); top < want {
		t.Errorf("the ladder tops out at %vms, below the %vms session ceiling — a percentile that "+
			"saturates reports a floor rather than a number", top, want)
	}
}
