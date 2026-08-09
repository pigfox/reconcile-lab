// Command sweep runs the contention sweep and prints the table, so the figures
// the pigfox.com page describes can be reproduced by anyone who clones this.
//
// It runs on the VIRTUAL clock, which is the point: the whole sweep finishes in
// under a second of wall time and produces the same numbers every run. The live
// page runs the same simulation on a real clock, and the two do not agree about
// one thing — see the README, and the note the page itself carries.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/pigfox/reconcile-lab/internal/reconcilelab"
)

func main() {
	lifetime := flag.Duration("lifetime", 5*time.Second, "simulated session length per arm")
	drift := flag.Int("drift", reconcilelab.MaxDriftRate, "competing writes per second")
	workers := flag.Int("workers", 2, "concurrent reconciles")
	sweepEvery := flag.Duration("sweep", time.Second, "periodic full resync interval")
	flag.Parse()

	objects := []int{32, 16, 8, 4, 2, 1}

	fmt.Printf("%-8s %-6s %-4s | %-14s %9s %9s %9s %8s %8s %6s\n",
		"objects", "drift", "wrk", "policy", "attempts", "landed", "conflicts", "wasted", "p95", "div")

	for _, n := range objects {
		for _, p := range reconcilelab.Policies() {
			cfg := reconcilelab.Config{
				Objects:       n,
				DriftRate:     *drift,
				Workers:       *workers,
				SweepInterval: *sweepEvery,
				Policy:        p,
				Lifetime:      *lifetime,
			}
			lab, err := reconcilelab.New(cfg, reconcilelab.NewVirtualClock(epoch))
			if err != nil {
				fmt.Fprintf(os.Stderr, "sweep: %v\n", err)
				os.Exit(1)
			}
			lab.Run(context.Background())
			s := lab.Snapshot()
			lab.Close()

			fmt.Printf("%-8d %-6d %-4d | %-14s %9d %9d %9d %8.4f %8v %6d\n",
				n, *drift, *workers, p, s.WriteAttempts, s.WritesLanded, s.Conflicts,
				s.WastedWriteRatio, s.P95, s.Diverged)
		}
	}
}

// epoch is a fixed start instant, so a run's timestamps are reproducible from
// the source alone rather than from when it happened to be run.
var epoch = time.Date(2026, time.August, 8, 12, 0, 0, 0, time.UTC)
