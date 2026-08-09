# reconcile-lab

The simulation behind **https://pigfox.com/demos/reconcile-lab**: a
Kubernetes-style reconcile loop and a competing writer contending for the same
object, with the loop's answer to a refused write as the only thing that varies.

Every write carries the `resourceVersion` it was read at, so the loser of a race
is refused with a real `apierrors.Conflict`. A controller has to decide what to
do about that, and there are only three answers:

| policy | what it does | what it costs |
|---|---|---|
| `retry-now` | re-read and write again, no delay | fastest back on target, most refused writes |
| `retry-backoff` | re-read and write again after a growing delay | fewer attempts, slower back on target |
| `stand-down` | abandon the pass and take **no write events at all** — the periodic sweep is the only way back to an object | wastes almost nothing, never fresher than one sweep |

## What is real here, and what is not

**Real:** `appsv1.Deployment`, `metav1.ObjectMeta`, the generated `DeepCopy`, the
`resourceVersion` rule, and the conflict error `apierrors.IsConflict` recognizes.
Two `k8s.io` modules and nothing else.

**Not real:** there is no API server. No etcd, no admission, no watch cache, no
network. This is the *write path* of an API server and nothing else of one — a
compare-and-swap object store in `store.go`, about 150 lines.

**No fake client, deliberately.** `k8s.io/client-go/kubernetes/fake` was measured
first: its object tracker assigns no `resourceVersion` at all — empty on read,
still empty after an update — and accepts a write carrying a stale version
without complaint. Every conflict on that substrate would have to be injected by
a reactor on a schedule the demo chose, and a conflict the demo arranged is not a
measurement of anything. The contention here arises from two goroutines racing.

## Time is injected

`Clock` is the package's only source of time and delay. Under `NewRealClock` the
simulation paces itself against the wall; under `VirtualClock` a ninety-second
session runs to completion in microseconds and produces the same numbers,
because virtual time only advances when every participating goroutine is
waiting. **No test in this package sleeps.**

A run is reproducible from its `Config`: the same controls and seed give
byte-identical results, asserted over twenty runs.

## The one place the two clocks disagree

Under the **virtual** clock, at one object, retrying at once becomes the *slower*
way back on target and backing off overtakes it — two reconciles running to the
microsecond settle into a rhythm where they keep colliding, and waiting is what
breaks the rhythm. `sweep_test.go` asserts that at both ends.

On a **real** clock it does not happen. Measured against the live page: retry-now's
p95 is 25 ms against backoff's 40 ms at one object, and 16 ms against 25 ms at
two. Timer overshoot and ordinary scheduler jitter de-synchronize the workers for
free, doing what backing off pays for.

Both are true, of different clocks. The page says which one its figures came
from, and so does this paragraph.

## Running it

```
make test     # race, shuffle, vet, gofmt
make cover    # 100% of statements in internal/reconcilelab
make sweep    # reproduce the contention table, ~1s on the virtual clock
```

## Licence

MIT, Pigfox LLC. See `LICENSE`.
