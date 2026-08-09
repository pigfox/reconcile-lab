package reconcilelab

import "time"

// histogram is a fixed-bucket histogram of restore latencies — how long an
// object stayed off target, from the write that moved it to the write that
// brought it back. There is no external dependency behind it and no per-sample
// retention: a contended session at the ceilings restores tens of thousands of
// times, and keeping every sample so a percentile could be exact would cost more
// memory than the simulation it is measuring.
//
// WHAT A PERCENTILE FROM IT MEANS, stated because a bucketed percentile is an
// approximation and reporting one as if it were exact is the usual way this kind
// of number misleads: quantile returns the UPPER BOUND of the bucket the
// quantile falls in, so it never understates. The one exception is the overflow
// bucket, where the ladder has no upper bound to report and the observed maximum
// is returned instead — which is the same promise, since the true value cannot
// exceed it.
type histogram struct {
	// counts has one slot per bound plus one for the overflow.
	counts []uint64
	total  uint64
	max    time.Duration
}

func newHistogram() *histogram {
	return &histogram{counts: make([]uint64, len(restoreBoundsMillis)+1)}
}

// observe records one restore latency.
func (h *histogram) observe(d time.Duration) {
	if d < 0 {
		d = 0
	}
	if d > h.max {
		h.max = d
	}
	h.total++

	ms := float64(d) / float64(time.Millisecond)
	for i, bound := range restoreBoundsMillis {
		if ms <= bound {
			h.counts[i]++
			return
		}
	}
	h.counts[len(h.counts)-1]++
}

// quantile returns the q-th quantile, q in [0,1]. It returns zero when nothing
// has been observed — a percentile over no restores is not a small number, it is
// the absence of one, and the page renders it as a dash rather than as 0 ms.
func (h *histogram) quantile(q float64) time.Duration {
	if h.total == 0 {
		return 0
	}
	if q < 0 {
		q = 0
	}
	if q > 1 {
		q = 1
	}

	// The rank of the sample being asked for, counted from one.
	target := uint64(q * float64(h.total))
	if target < 1 {
		target = 1
	}

	// The overflow slot is the default rather than a trailing fallback, so this
	// function has no statement that cannot be reached: the loop always finds the
	// target (target is at most h.total), and the only way to land on the last
	// index is a genuine overflow.
	idx := len(h.counts) - 1
	var seen uint64
	for i, c := range h.counts {
		seen += c
		if seen >= target {
			idx = i
			break
		}
	}
	if idx == len(h.counts)-1 {
		return h.max
	}
	return time.Duration(restoreBoundsMillis[idx] * float64(time.Millisecond))
}
