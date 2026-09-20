# gosched — a work-stealing scheduler whose decisions are observable

`gosched` is a work-stealing task scheduler in Go (standard library only) built so
that its scheduling decisions can be inspected after the fact. The scheduler
itself — per-P workers, Chase-Lev deques, a shared overflow queue, park/unpark, a
fork-join `Wait` — is table stakes. What makes the project worth reading is the
second half: every steal, migration, park, block and task execution is recorded
into per-worker ring buffers, exported to Chrome Trace Event Format, and
aggregated into a plain-text report a human can argue with.

The question it is built to answer is not "is it fast" but **"why was it fast, and
where exactly was it not"** — with recorded evidence.

---

## The honest headline

On this machine (Apple M4 Pro, 12 logical cores, 8 performance + 4 efficiency),
across 7 interleaved repetitions per cell, medians quoted
(`results/bench-summary.txt`, raw runs in `results/bench.csv`):

| workload | P | naive `go func()` | fixed channel pool | **gosched** | gosched, tracing off |
|---|---|---|---|---|---|
| uniform, 6000×100µs | 12 | 48 425 | 44 977 | **49 662** | 45 276 |
| fork-join, 8191 tasks | 12 | 107 888 | 101 716 | **102 540** | 113 571 |
| skewed, 40 long + 4000 short | 12 | 92 878 | 89 370 | **93 561** | 86 561 |
| blocking, 800×250µs sleep | 12 | 1 077 441 | 41 085 | **66 295** | 73 294 |

(tasks/sec; P = worker count = `GOMAXPROCS`)

**On the uniform and skewed workloads the three implementations are
statistically indistinguishable.** The run-to-run spread in those cells is tens of
percent (`results/bench-variance.txt`), and the differences between
implementations are smaller than that spread. A work-stealing scheduler with
per-P deques does not beat `go func()` on independent uniform tasks: the Go
runtime's own scheduler already spreads those goroutines across cores, and this
scheduler's advantage — owning work locally, stealing only when a worker runs dry
— has nothing to rebalance when every task is identical and the shared queue
hands out equal batches.

The two places where the design *does* separate from the baselines:

* **Cooperative blocking.** At P=1 the scheduler runs 4.8× the fixed pool
  (16 358 vs 3 428 tasks/sec) and at P=12 it is 1.6× (66 295 vs 41 085), because a
  task that declares a wait hands its queue over and the pool grows a helper
  instead of losing the core. The naive baseline wins that workload outright
  (1 077 441 tasks/sec) because it has unbounded concurrency: 800 sleeping
  goroutines cost it nothing, and no bounded pool can match that. That is a real
  property of the baseline, not a rigged one.
* **Tail latency under skew** (the case stealing is supposed to win), p99 of
  submit-to-completion, median of 7 runs at P=12: naive 38.5 ms, pool 39.4 ms,
  **sched 28.6 ms**. The scheduler's best observed p99 at P=10/P=12 is
  20.8 ms/16.8 ms against 30.4 ms/34.5 ms for naive. This is the mechanism
  working — a worker stuck on a 4 ms task loses its queued short tasks to an idle
  peer — but the cells are noisy, so it is reported as a visible-but-noisy
  advantage rather than a clean win (`results/skewed-latency.txt`).

Everything below explains how the scheduler works, what the tests falsify, and
where every number came from.

---

## Commands

```bash
make test          # go test ./...
make test-race     # go test -race ./...
make test-stress   # go test -race -count=5 ./...   (lost-wakeup / stability)
make bench         # go test -run XXX -bench=. -benchtime=2s ./bench/...
make results       # full measurement matrix -> results/
make trace-report  # aggregate analysis of results/example-trace.json
make verify        # build + vet + gofmt check + race + stress
```

Everything is standard-library only; `go.mod` declares `go 1.22`.

The equivalent explicit commands, which is what was actually run for this
README:

```bash
go build ./...
go vet ./...
go test -race ./...
go test -race -count=5 ./...
go test -run XXX -bench=. -benchtime=2s ./...
go run ./cmd/gosched-bench -reps 7 -procs 1,2,4,8,10,12 -out results
go run ./cmd/gosched-trace -in results/example-trace.json
```

---

## How it works

### 1. The deque (`sched/deque.go`)

Each P owns a Chase-Lev deque: the owner pushes and pops at one end (LIFO), any
number of thieves steal from the other (FIFO). LIFO at the owner end gives
depth-first fork-join behaviour, which keeps a task's siblings on the core that
is already warm for them; FIFO at the thief end hands out the coldest task.

Growth is amortised doubling. Two details are worth calling out because they are
where this class of data structure usually goes wrong:

* **The capacity invariant.** The owner grows before it would write slot `b` while
  `b - top >= len(slots)`. That is what guarantees the live range `[top, bottom)`
  always occupies distinct slots, so a thief reading slot `t` and the owner
  writing slot `b` can never be describing the same memory.
* **Slots are accessed atomically.** A thief reads `top` before `bottom`, so its
  `top` can be stale; if other thieves meanwhile advance `top` and the owner wraps
  the buffer, the owner's write and the thief's read hit the same slot. The value
  read is always discarded in that case (the CAS on `top` fails), so the algorithm
  stays correct — but in Go terms it is a plain data race, i.e. undefined
  behaviour, and the race detector says so. Atomic slot loads/stores make it
  benign by construction and are also what makes the deque sound on a weakly
  ordered machine like arm64.

The deque stores untyped words and ships with two thin typed skins (`Task` and the
traced scheduler's `workItem`), both one pointer wide, so neither needs a second
slot or an allocation.

### 2. Where work enters: the shared overflow queue (`sched/global.go`)

Every submission lands in a sharded FIFO (`sync.Mutex` per shard, a round-robin
ticket to pick one). A worker that has nothing local claims *half* of what is
queued, capped at 256, into its own deque.

Two design consequences were measured rather than assumed:

* **Sharding matters.** At 12 cores a fork-join workload submits twice per task;
  a single lock for all of them would serialise the whole pool. The shard count is
  `2×P` rounded to a power of two, capped at 32.
* **Claiming is what fills local deques, and local deques are what stealing
  balances.** Claiming one item at a time turns this into a plain shared-queue
  pool in which stealing never fires because nobody ever holds work: measured
  steal success dropped to 0/4 probes in the skew test with small claims. Claiming
  half produced 26–53% steal success rates (`results/bench-summary.txt`,
  "scheduler decisions" table).

Why don't tasks submitted from *inside* a task go to that worker's own deque, as
they would in an ideal design? Because Go has no goroutine-local storage, and the
only portable way to identify the calling worker is to parse its goroutine id out
of `runtime.Stack`, which measured at **543 ns/call** on this machine — roughly
twenty times the cost of an entire submit. That measurement, and the resulting
attribution rule, is the reason the trace marks in-task submissions as coming from
the shared queue rather than from a P. `sched/goid.go` documents the alternatives
that were rejected and why.

### 3. The wakeup protocol (`sched/note.go`) — where lost wakeups live

An idle worker publishes "I am parking", re-checks for work, and only then blocks.
The re-check after the publish is the whole trick: a submitter that reads a
non-parking state is guaranteed to have queued its task *before* that read, and
therefore before the check that follows, so the worker sees it. Submitters read
the state after pushing and wake a parked worker if they find one.

Blocking itself goes through a `note`: a one-shot wakeup register with three
states (idle / sleeping / pending). A token that arrives while the worker is
running is *remembered*, so the next sleep returns immediately instead of
blocking forever — which is exactly what a plain buffered-channel handshake gets
wrong, because a second token arriving while the buffer is full is silently
dropped.

The note is a notification, not a count: two wakes that arrive before the sleeper
consumes the first collapse into one. That is sound here precisely because every
wake is followed by a re-check of every queue, and it is documented on the type so
nobody later mistakes it for a semaphore.

### 4. `Wait` and the transitive closure

`Wait` returns when the pending count — incremented *before* a task becomes
visible and decremented after it returns — reaches zero. Children submitted while
a task executes increment the same counter, so the closure is covered by
construction. Waiters register a channel under a mutex and re-check the count
under that same mutex, which closes the race between "the last task finished just
before I registered" and "I block forever".

### 5. Blocking handoff and helper workers

`Blocking(fn)` is the cooperative half of blocking detection:

1. the calling worker's local queue is published to the shared queue (so other
   workers can take it), and parked workers are woken to come and take it;
2. if every executor is currently blocked, a transient helper worker is started
   so the pool cannot deadlock on work only a blocked worker could run;
3. `Block`/`Unblock` events are recorded, including how many queued tasks were
   handed off.

The helper spawn/retire decision is serialised by a mutex. Without that, there is
a window in which a retiring helper has decided to stop but has not yet returned
its slot, and a worker that has just blocked finds no helper running and no free
slot; the unit test caught exactly that window before it was closed.

### 6. Tracing: per-worker rings with exact counters (`trace/`)

* Nine event types: `Submit`, `Steal`, `StealFail`, `Migrate`, `Park`, `Unpark`,
  `Execute`, `Block`, `Unblock`.
* One bounded ring per worker, plus one multi-writer ring for submissions from
  goroutines the scheduler does not own. Appends take no lock on the hot path: a
  ring publishes its length with one atomic store, and the shared ring claims a
  slot and publishes it individually (the race detector caught the version that
  assumed a single writer).
* **Detail is lossy; totals are not.** Every ring also maintains plain atomic
  counters, so the report's steal counts, migration counts, per-P utilisation and
  handoff totals are exact even when events are dropped. Dropped events are
  counted exactly, per ring, not estimated.
* Rings allocate on first record, so a scheduler with tracing off — or a helper
  that never ran — costs no memory. A real P gets 65 536 events (2.6 MB); a
  transient helper gets 1024; the submission sink gets 65 536.

The drop policy is stated precisely in **Limitations** below.

### 7. Why the trace format is the interesting part

A benchmark can tell you a scheduler was faster. It cannot tell you *which
mechanism* did it, and it cannot distinguish "the work stealing helped" from "the
machine was warm that run". The trace can: it records who probed whom, how deep
the victim's queue was, who actually ran each task, how long each task waited,
and which worker was asleep when.

The specific piece of design that makes the analysis more than a log is the
**holder** field on a traced task: the P that most recently owned it in a local
queue, or "unclaimed" while it sits in the shared queue. A task executed by a P
that is not its holder crossed a worker boundary, by stealing or by a blocking
handoff, and the trace records that as a `Migrate` with the origin. This is how
"migrations" stays a meaningful, exactly-computed number instead of a guess.

### 8. Chrome Trace Event export and the aggregate report

`trace.WriteChrome` writes task executions as complete (`X`) events (so the viewer
draws a duration per task) and decisions as thread-scoped instant (`i`) events on
the P that made them. The file carries the exact counters as a `gosched_stats`
metadata event, which means a trace whose rings overflowed still round-trips into
the same aggregate report. `trace.ParseChrome` reads it back, and the report tool
is deliberately pointed at the *file* rather than at a live recorder: if the
export dropped a field the analysis needs, the report cannot be produced.

---

## Reading the committed example trace

`results/example-trace.json` (159 KB, 1260 events, **0 dropped**) is a capture of a
fork-join tree plus a skewed burst at P=4, 533 tasks, re-imported and analysed in
`results/example-trace-report.txt`:

| observation | value | what it says |
|---|---|---|
| steal attempts / successes | 134 / 52 → **38.8%** | thieves found work about two times in five |
| probes absorbed by empty victims | 82 | the other three probes in five hit an empty deque |
| execution migrations | 52 (9.8% of executions) | exactly the steal successes: every transferred task ran somewhere other than where it was claimed |
| mean / max local queue depth | 27.1 / 132 | claiming half the shared queue produces genuinely deep local deques, which is what gives stealing something to do |
| per-P utilisation | 90.0%–90.9% (mean 90.5%) | all four workers were busy for essentially the whole run; work was not sitting in one deque |
| park / unpark | 4 / 4 | each worker parked once, at the end |
| donated work (stolen out) | P2: 43, P1: 9, P0/P3: 0 | the imbalance is visible per worker: P2 hoarded, then gave it away |

That last row is the kind of statement only the trace can support, and it is the
answer to "why was it faster": the run was balanced *by transfers between
workers*, and the transfer count is right there.

Open the file in `chrome://tracing` or <https://ui.perfetto.dev>; timestamps in
the file are microseconds with nanosecond resolution and the JSON object declares
`displayTimeUnit: ns`.

---

## Tests: what each one is built to falsify

| test | falsifies |
|---|---|
| `TestDequeMatchesReferenceModel` | LIFO/FIFO end semantics and empty-transitions, against a slice model, 100k random ops |
| `TestDequeGrowthUnderContention` | growth while thieves drain: double-CAS, stale `top` during copy |
| `TestDequeConcurrentExactlyOnce` | duplication *and* loss under owner/thief contention, per-task counters as the model |
| `TestDequeStealOnEmptyAndSharedSteal` | degenerate paths: steal from empty, four thieves racing one entry |
| `TestCorrectnessUnderLoad` | exactly-once execution and correct per-task results over an 8191-task fork-join tree, traced and untraced |
| `TestWaitCoversTransitiveClosure` | `Wait` returning before children submitted *during* execution have run; checks for stragglers after return |
| `TestNoLostWakeups` | 8 submitters against 2 workers with sleep gaps that force parking; asserts all tasks ran *and* that workers actually parked, so the test cannot pass without exercising the race |
| `TestNoteNeverStrandsASleeper` | the wakeup primitive under wake-while-running, wake-while-blocked, coalescing and a 4-goroutine wake storm |
| `TestStealFairnessUnderSkew` | no stealing, or a starved worker, under a skewed workload (steal rate > 1%, least-busy worker ≥ 35% of the busiest) |
| `TestQueuedWorkIsStealableWhileOwnerIsBusy` | a worker holding a queue while pinned by a long task keeping it all |
| `TestBlockingHandoff` | pool deadlock when every P is blocked, and that handed-off work is really executed elsewhere |
| `TestTraceIsDeterministicWithSingleP` | trace non-determinism: identical decision sequences (timestamps, durations and waits excluded) across runs with a fixed seed |
| `bench`: `TestShapesTaskCountsAreExact` | the benchmark's own denominators: a shape must run exactly the number of tasks it reports |
| `bench`: `TestForkJoinOnEveryImpl` | the fixed pool deadlocking on fork-join, and any implementation losing tasks |

Three of these found real bugs during development, which is the point of writing
them to falsify rather than to confirm:

1. The race detector found the stale-slot read in the deque described in §1.
2. The park path called `findWork()` to check for work and **discarded the
   returned task** — but `findWork` had already dequeued it. Tasks were silently
   lost; the smoke test showed 142 tasks pending with every queue empty.
3. Workers started in `New` and could park before the first `Submit`, which made
   the single-P trace non-reproducible (1 or 2 park/unpark pairs). Workers now
   start lazily, just after the first task is queued.

---

## Measurements

### Machine

```
go version      : go1.27.1
os/arch         : darwin/arm64 (macOS 15.7.7)
logical cores   : 12
gomaxprocs      : 12
cpu             : Apple M4 Pro
core topology   : 8 performance + 4 efficiency cores
```

(`results/machine.txt`)

### Methodology

* Four workloads (`bench/workload.go`), three implementations plus the scheduler
  with tracing off, so instrumentation cost can be separated from scheduling cost.
* Each cell is the **median of 7 repetitions**; repetitions interleave the
  implementations, and each cell is warmed up first, so a cool CPU or a
  systematic ordering cannot favour one implementation.
* Setup, submission, execution and teardown are all inside the measured window
  for every implementation.
* All three implementations run *identical* work: the same `Shape` drives all of
  them and no baseline is handicapped. The pool baseline uses a 16 384-entry
  buffered channel (a smaller buffer turns fork-join into a deadlock rather than a
  measurement) and all workers are counted in teardown.
* `results/bench-variance.txt` records min/median/max per cell. **Where that
  spread is large the medians in this README are not precise, and the text says
  so.** Across all 96 cells, the median cell spread is 2.7% at P=2 and 10.2% at
  P=4, rising to 31.2% at P=10 and 39.0% at P=12; the worst cells (the naive
  blocking cells, whose whole run is under a millisecond, and the uniform cells
  at P≥8) reach 52–89%. Any comparison in this README that is inside the spread
  of its own cell is described as a tie rather than a win.

### Throughput and speedup

Full tables: `results/bench-summary.txt`. Speedups are relative to P=1 for the
same implementation.

Uniform (6000 tasks × 100µs):

| impl | P=1 | P=2 | P=4 | P=8 | P=10 | P=12 |
|---|---|---|---|---|---|---|
| naive | 9 870 | 19 535 | 39 053 | 45 320 | 68 201 | 48 425 |
| pool | 9 951 | 19 841 | 38 855 | 47 787 | 63 437 | 44 977 |
| sched | 9 889 | 19 665 | 38 340 | 45 478 | 65 316 | 49 662 |
| sched-notrace | 9 910 | 19 861 | 39 395 | 42 080 | 69 537 | 45 276 |

Scaling is ~2.0× per doubling to P=4 (3.9×), then saturates: 4.6–7.0× at P=12.
The `P=10 → P=12` dip visible in these medians is **not a reproducible effect**:
those cells have 26–89% run-to-run spread, two of the four implementations go
*up* from P=10 to P=12 in the other workloads, and the pure-CPU scaling probe
below shows the machine cannot deliver 12× anyway.

Skewed (40 tasks × 4 ms + 4000 × 25 µs), and fork-join (8191 tasks in a binary
tree): same file, same conclusion — everything within a few percent of everything
else, with the scheduler's own numbers never worse than ~10% below the best
baseline.

Blocking (800 tasks × 250µs sleep):

| impl | P=1 | P=2 | P=4 | P=8 | P=10 | P=12 |
|---|---|---|---|---|---|---|
| naive | 654 999 | 1 074 006 | 1 038 005 | 1 048 780 | 1 096 829 | 1 077 441 |
| pool | 3 428 | 6 884 | 13 720 | 27 357 | 34 356 | 41 085 |
| sched | 16 358 | 19 496 | 25 874 | 49 267 | 60 471 | 66 295 |
| sched-notrace | 16 836 | 19 951 | 26 921 | 54 258 | 64 708 | 73 294 |

The naive baseline is not a fair fight — it has unbounded concurrency and sleeping
costs it nothing — but the comparison against the *bounded* pool is the one that
matters for a task pool, and the scheduler wins it at every P (4.8× at P=1).

### Tail latency (skewed workload)

`results/skewed-latency.txt`. Latency is measured per task from the submit call to
the task's return, so for the pool and the scheduler it includes queueing time.

| p99, P=12 (median of 7) | naive | pool | sched | sched-notrace |
|---|---|---|---|---|
| skewed | 38.5 ms | 39.4 ms | **28.6 ms** | 30.7 ms |

At P=10: naive 41.7 ms, pool 40.9 ms, sched 25.9 ms. The scheduler's best observed
p99 at P=10/P=12 is 20.8 ms/16.8 ms against 30.4 ms/34.5 ms for naive, i.e. the
direction is consistent even though individual cells are noisy. This is the
mechanism from §2 doing its job: a worker pinned by a 4 ms task loses its queued
short tasks to an idle peer instead of holding them.

### Tracing overhead

`results/trace-overhead.txt`, sched vs sched-notrace, median of 7:

| workload | P=1 | P=4 | P=8 | P=12 |
|---|---|---|---|---|
| uniform | +0.2% | +2.7% | −8.1% | −9.7% |
| fork-join | +0.8% | +9.9% | **+21.4%** | +9.7% |
| skewed | +3.2% | −1.1% | +4.7% | −8.1% |
| blocking | +2.8% | +3.9% | +9.2% | +9.5% |

Two cells are *negative*, i.e. tracing appears faster than not tracing. That is
physically impossible and it means those cells are noise-dominated (spread 36% and
37% at uniform P=12; 66%/89% at P=8). The defensible reading: **where the cell is
measurable, tracing costs roughly 3–10%, with the worst measurable case +21.4% on
fork-join at P=8** — fork-join has the highest event rate (two submissions and one
execution per task), which is exactly where instrumentation should hurt most.
Isolated, `BenchmarkTracerRecord` prices one ring append plus one exact-counter
update, with and without the clock read that stamps it.

### What the scheduler actually did

Full table in `results/bench-summary.txt`; steal success rate (successes /
attempts, median run of each cell):

| workload | P=2 | P=4 | P=8 | P=10 | P=12 |
|---|---|---|---|---|---|
| uniform | 0/2 | 34/103 = 33.0% | 263/735 = 35.8% | 235/846 = 27.8% | 409/1005 = 40.7% |
| fork-join | 0/3 | 78/147 = 53.1% | 121/528 = 22.9% | 149/582 = 25.6% | 169/971 = 17.4% |
| skewed | 6/10 = 60.0% | 53/135 = 39.3% | 455/1146 = 39.7% | 1292/4486 = 28.8% | 1334/5116 = 26.1% |
| blocking | 0/9 | 2/53 = 3.8% | 0/381 | 2/372 = 0.5% | 0/505 |

Read this together with the throughput tables and it explains them:

* At P≤2 there is nothing to steal: one claimer takes everything, so attempts are
  in the single digits. **The uniform benchmark therefore cannot distinguish the
  implementations — not because nothing happens, but because nothing *needs* to
  happen.**
* Stealing is common at P≥4 (17–53%) in every CPU-bound workload, and at P=12 the
  skewed workload performs 1334 successful steals out of 5116 probes.
* The blocking workload steals almost never (0.4% at P=12) because the mechanism
  there is different: `results/bench-summary.txt` shows **79 337–138 465 tasks
  handed off** by blocking workers, against 0–8 steals. When a task announces a
  wait, work is published rather than stolen.
* Zero events were dropped in every measured cell (`trace_dropped = 0` in
  `results/bench.csv`), so these numbers come from complete traces.

### Why speedup saturates: a probe, not a guess

The uniform workload's speedup stops at ~5× on 12 cores. Two explanations were
possible — the machine — or the harness's own work unit, a wall-clock spin that
reads the clock in a tight loop, which is a shared contended read at high thread
counts. `results/core-scaling-probe.txt` separates them:

| threads | clock spin, per-thread rate | arithmetic work, per-thread rate |
|---|---|---|
| 1 | 499 /s | 501 /s |
| 4 | 499 /s | 460 /s |
| 8 | 492 /s | 359 /s |
| 10 | 467 /s | 292 /s |
| 12 | 393 /s | 241 /s |

The clock-based unit holds ~99% per-thread efficiency to 10 threads, so the
harness is not the limit. The clock-free arithmetic unit — a pure dependent
multiply-add chain with no shared state — falls to **48% per-thread efficiency at
12 threads (5.8× aggregate)**. In other words, on this machine 12 threads deliver
about 5.8× of one thread's throughput for pure CPU work (single-core turbo versus
all-core sustained clocks, plus four efficiency cores), and the uniform workload's
measured 4.6–7.0× speedup sits exactly at that ceiling. The scheduler is not what
is saturating; the hardware is. That also means the P=10→P=12 dip is not an
efficiency-core cliff — the workloads that are long enough to measure cleanly
(fork-join, skewed) *increase* from P=10 to P=12.

---

## Limitations

Honest and specific. Several of these are consequences of deliberate design
choices, not oversights.

1. **The event buffers are lossy under overflow.** Each real P retains up to
   65 536 events (2.6 MB), each transient helper 1024, and the submission sink
   65 536. Beyond that the *newest* events for that ring are dropped and counted
   exactly: `trace_dropped` in `results/bench.csv` is the true number lost, and it
   was 0 in every published cell. The number of events that *can* be lost is
   unbounded in principle (drops accumulate per ring as long as recording
   continues), which is why the aggregates are maintained as separate exact
   counters rather than derived from the (lossy) event stream. A trace that drops
   events still reports correct steal, migration, handoff, park and utilisation
   totals; it loses detail, not truth.
2. **Task identity costs an allocation.** With tracing on, each submission
   allocates a 32-byte item to carry the task id, origin and submit time. With
   tracing off the deques hold bare `func()` values and there is no allocation.
   That is part of the measured tracing overhead, not separate from it.
3. **A submitting worker cannot be identified.** Go has no goroutine-local
   storage. Tasks submitted from inside a task therefore enter the shared queue
   and are attributed to it; they do not land on the submitting worker's deque as
   they would in an ideal design. `Blocking` pays 543 ns to parse a goroutine id
   out of `runtime.Stack` to find the caller's queue, which is acceptable for a
   call that is about to block and unaffordable on `Submit`. This is the single
   biggest deviation from a textbook work-stealing scheduler, and it is why
   claiming, not stealing, is the primary distributor.
4. **The claim size is a heuristic.** Half the shared queue, capped at 256. Both
   the half and the cap are policy choices with no adaptive feedback; a work
   profile with very uneven task durations would want a different one.
5. **No priority, deadlines, cancellation or task results.** A task is a `func()`
   that returns nothing; results travel through closures the caller captured.
6. **No preemption of long tasks.** A 4 ms task holds its worker for 4 ms. There
   is no work-first/help-first alternative, and no way to split a long task.
7. **Blocking handoff is cooperative, not automatic.** A task that blocks without
   calling `Blocking` is invisible to the scheduler. It will not deadlock — its
   queued work is still stealable, and the unit tests cover that — but it will not
   trigger a helper either. If more than `maxHelpers` executors block at once
   (default: one per P, at least 4), progress depends on one of them returning.
8. **No CPU affinity, and no use of the 8P+4E topology.** Workers are pinned to
   nothing. On this machine that leaves performance on the table and adds
   variance: the P=12 cells have far more spread than P=4.
9. **`GOMAXPROCS` is assumed, not controlled.** The default worker count is
   `runtime.GOMAXPROCS(0)`; the harness sets both explicitly for the sweep, but
   nothing prevents a caller from running a 12-worker scheduler on a 2-core
   machine other than the default.
10. **Helpers poll.** Transient helpers sleep 50 µs between checks instead of
    having a real park/unpark path, which bounds how quickly blocked work is
    picked up in the all-blocked case.
11. **The trace is a capture, not a live stream.** `Snapshot` requires a quiescent
    scheduler (after `Wait` or `Close`); there is no incremental exporter and no
    way to watch a long-running pool.
12. **Timestamps come from `time.Now`.** They are monotonic but coarse (~40 ns on
    macOS) and events that land in the same tick are ordered arbitrarily by the
    merge sort. The determinism test is explicit about this: decision sequences
    are compared, timestamps and durations are not.
13. **One process, one machine.** There is no work stealing across processes, no
    NUMA awareness, and the scheduler has no idea whether a task is I/O-bound or
    CPU-bound.
14. **A panicking task takes the process down** (the worker goroutine does not
    recover). There is no error channel and no supervision.

## What I'd do next

1. **Give tasks a handle** (`func(*Worker)` or a context) so in-task submissions
   can go to the submitting worker's deque. That single change would turn this
   into a genuine local-first work-stealing scheduler and would let children be
   attributed to a P in the trace — and it is worth an API break.
2. **Adaptive claiming.** Feed the measured steal-failure rate back into the
   claim size: claim less when thieves are starving, more when the queue is deep
   and workers are busy.
3. **Work-first blocking.** Run a queued sibling while a task waits, instead of
   handing the queue off and hoping a peer picks it up.
4. **E-core awareness.** Let the scheduler know the machine's topology and prefer
   to keep long tasks off efficiency cores, then measure whether it reduces the
   variance that currently dominates the P≥8 cells.
5. **A live, non-lossy exporter.** A lock-free MPMC queue per worker plus an
   exporter goroutine would allow streaming traces without a quiescent snapshot,
   and would remove limitation 1.
6. **Chunked stealing** (steal half a victim's deque rather than one task), which
   is the standard fix for the tail latency this design already improves.
7. **Real tail-latency numbers at scale.** The p99 measurement here is one sample
   per run; a histogram-per-task with more repetitions would say whether the
   28.6 ms vs 38.5 ms gap at P=12 is stable.
8. **Measure with hardware counters** (instructions, cycles, cache misses) rather
   than wall-clock spins, which would remove the work-unit question entirely.
9. **Compare against a real thread pool's moral equivalent** — `errgroup` with a
   semaphore, or a `ForkJoinPool`-style work-first pool — instead of only the two
   baselines here.

---

## Repository layout

```
sched/     deque.go global.go note.go goid.go sched.go   the scheduler
           *_test.go                                     tests, incl. reference-model deque tests
trace/     event.go tracer.go chrome.go report.go         instrumentation, export, analysis
bench/     workload.go run.go bench_test.go               workloads, baselines, benchmarks
cmd/       gosched-bench/  gosched-trace/                 measurement harness, report CLI
results/   machine.txt bench.csv bench-summary.txt bench-variance.txt
           trace-overhead.txt skewed-latency.txt core-scaling-probe.txt
           example-trace.json example-trace-report.txt example-trace-info.txt
```

### Traceability of the numbers in this README

| claim | file |
|---|---|
| machine specs, core topology, Go version | `results/machine.txt` |
| every individual measured run (672 rows) | `results/bench.csv` |
| throughput, speedup and scheduler telemetry medians | `results/bench-summary.txt` |
| run-to-run spread per cell | `results/bench-variance.txt` |
| tracing overhead percentages | `results/trace-overhead.txt` |
| p50/p99/max latency per implementation and P | `results/skewed-latency.txt` |
| work-unit scaling probe (why speedup saturates) | `results/core-scaling-probe.txt` |
| the committed trace and its aggregate reading | `results/example-trace.json`, `results/example-trace-report.txt`, `results/example-trace-info.txt` |
