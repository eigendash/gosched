package sched

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/eigendash/gosched/trace"
)

// spinFor burns CPU for at least d. A sleeping task would not occupy its P, and
// the whole point of the skewed workload is that a long task pins a worker
// while short tasks pile up behind it.
func spinFor(d time.Duration) {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
	}
}

func waitFor(t *testing.T, what string, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		runtime.Gosched()
	}
}

// skewOutcome is one run of the skewed workload, including how long the
// producer needed to hand the work over. That number matters: if the producing
// goroutine is starved by the OS, the pool is idle because the work has not
// arrived yet, and utilisation then measures the producer rather than the
// scheduler.
type skewOutcome struct {
	rep        *trace.Report
	completed  int64
	want       int64
	submitWall time.Duration
	wall       time.Duration
	perWorker  []int64
}

// valid reports whether the run says anything about the scheduler: the producer
// must not have been the bottleneck.
func (o skewOutcome) valid() bool {
	return o.submitWall*2 < o.wall
}

// skewAttempt runs the skewed workload once. Four producer goroutines submit
// contiguous chunks, because a single submitter is itself a bottleneck on a
// loaded machine and its stalls would look exactly like scheduler starvation.
func skewAttempt(procs int) skewOutcome {
	const (
		longTasks  = 16
		longWork   = 4 * time.Millisecond
		shortTasks = 2400
		shortWork  = 200 * time.Microsecond
		producers  = 4
	)
	total := longTasks + shortTasks
	// A small ring is deliberate: the analysis reads the exact counters, so the
	// event budget changes nothing about the result, while a million-event ring
	// per worker makes the allocator (not the scheduler) the thing being
	// measured.
	s := New(WithProcs(procs), WithSeed(7), WithTraceCapacity(1<<14))
	var completed int64

	start := time.Now()
	var wg sync.WaitGroup
	per := total / producers
	for p := 0; p < producers; p++ {
		lo, hi := p*per, (p+1)*per
		if p == producers-1 {
			hi = total
		}
		wg.Add(1)
		go func(lo, hi int) {
			defer wg.Done()
			// Long work is spread evenly through the stream (every 151st task)
			// so neither kind clusters at one end of the queue: a worker that
			// claims a batch gets both.
			for i := lo; i < hi; i++ {
				if i%151 == 0 {
					_ = s.Submit(func() {
						spinFor(longWork)
						atomic.AddInt64(&completed, 1)
					})
					continue
				}
				_ = s.Submit(func() {
					spinFor(shortWork)
					atomic.AddInt64(&completed, 1)
				})
			}
		}(lo, hi)
	}
	wg.Wait()
	submitWall := time.Since(start)
	s.Wait()
	wall := time.Since(start)

	rec := s.Trace()
	rep := trace.Analyze(rec)
	perWorker := make([]int64, len(rep.PerP))
	for i := range rep.PerP {
		perWorker[i] = rep.PerP[i].Executed
	}
	s.Close()
	return skewOutcome{
		rep:        rep,
		completed:  atomic.LoadInt64(&completed),
		want:       int64(total),
		submitWall: submitWall,
		wall:       wall,
		perWorker:  perWorker,
	}
}

// TestStealFairnessUnderSkew is the fairness claim made concrete: under a
// workload where a few long tasks pin part of the pool while thousands of short
// tasks are queued, no worker may be starved, and when the pool is genuinely
// imbalanced stealing must be what fixes it.
//
// The property is statistical, and the machine is shared with the other test
// binaries when the suite runs as `go test ./...`. A run in which the OS simply
// never scheduled one worker looks identical to scheduler starvation from the
// inside, so the scenario is retried and the assertion is made against the best
// attempt: a scheduler that never balances fails every attempt, a scheduler that
// balances fails none.
func TestStealFairnessUnderSkew(t *testing.T) {
	for _, procs := range []int{4, 8} {
		t.Run(procName(procs), func(t *testing.T) {
			const attempts = 3
			var best *skewOutcome
			bestShare := -1.0
			valid := 0
			for attempt := 0; attempt < attempts; attempt++ {
				out := skewAttempt(procs)
				if out.completed != out.want {
					t.Fatalf("attempt %d: completed %d tasks, want %d", attempt, out.completed, out.want)
				}
				share := 1.0
				if out.rep.MaxUtilization > 0 {
					share = out.rep.MinUtilization / out.rep.MaxUtilization
				}
				t.Logf("attempt %d procs=%d valid=%v submit=%.1fms wall=%.1fms steals=%d/%d (%.1f%%) migrations=%d util mean=%.0f%% min=%.0f%% max=%.0f%% work-share=%.2f per-worker=%v",
					attempt, procs, out.valid(),
					float64(out.submitWall)/1e6, float64(out.wall)/1e6,
					out.rep.Totals.StealSuccesses, out.rep.Totals.StealAttempts, out.rep.StealSuccessRate*100,
					out.rep.Totals.MigrationsIn,
					out.rep.MeanUtilization*100, out.rep.MinUtilization*100, out.rep.MaxUtilization*100,
					share, out.perWorker)
				if !out.valid() {
					// The producer used more than half the wall clock, so the
					// pool was mostly waiting for work to exist. That is a fact
					// about the machine, not about the scheduler, and using it
					// as evidence either way would be wrong.
					continue
				}
				valid++
				if share > bestShare {
					bestShare = share
					b := out
					best = &b
				}
			}
			if valid == 0 || best == nil {
				t.Fatalf("none of %d attempts produced a usable measurement: the producing goroutines used more than half the wall clock, so the pool was starved of input rather than of workers", attempts)
			}

			// Starvation check. It compares the least-busy worker's measured work
			// against the busiest one rather than against the wall clock: both
			// are wall-time-normalised, so the ratio is "share of the work",
			// which survives the slowdown of running under -race, while an
			// absolute utilisation floor would not.
			if bestShare < 0.35 {
				t.Fatalf("best of %d valid attempts: least-used worker did only %.0f%% of the busiest worker's work (min=%.1f%% max=%.1f%% per-worker=%v)",
					valid, bestShare*100, best.rep.MinUtilization*100, best.rep.MaxUtilization*100, best.perWorker)
			}
			// Stealing must be what balances an imbalanced pool. If claiming
			// distributed the work perfectly there is nothing to steal, and
			// demanding a steal would make the test depend on timing rather than
			// on the scheduler.
			if bestShare < 0.90 {
				if best.rep.Totals.StealSuccesses == 0 {
					t.Fatalf("pool was imbalanced (work-share %.2f, per-worker=%v) and %d probes found nothing to steal",
						bestShare, best.perWorker, best.rep.Totals.StealAttempts)
				}
				if best.rep.StealSuccessRate < 0.01 {
					t.Fatalf("steal success rate %.3f%% is below the 1%% floor on an imbalanced pool",
						best.rep.StealSuccessRate*100)
				}
			}
		})
	}
}

// TestQueuedWorkIsStealableWhileOwnerIsBusy covers the structure that makes
// stealing worth having: a worker holding queued tasks must be able to lose
// them to an idle peer even though it is not cooperating in any way (it never
// blocks, never yields, and the scheduler is never told it is busy).
//
// Whether the long task lands in the middle of a claimed batch (and therefore
// keeps the older tasks of that batch behind it) depends on where the batch
// boundaries fell, which is timing. The test therefore varies the long task's
// position across attempts and requires the mechanism to fire in at least one of
// them: a scheduler that cannot hand work over fails all of them.
func TestQueuedWorkIsStealableWhileOwnerIsBusy(t *testing.T) {
	const (
		procs      = 2
		shortTasks = 120
		shortWork  = 200 * time.Microsecond
		longWork   = 40 * time.Millisecond
		attempts   = 4
	)
	var steals, attemptsCount int64
	var worst string
	for attempt := 0; attempt < attempts; attempt++ {
		// Move the long task through the stream, one batch-position per attempt.
		before := 5 + attempt*17
		s := New(WithProcs(procs), WithSeed(3), WithTraceCapacity(1<<14))
		var completed int64
		short := func() {
			spinFor(shortWork)
			atomic.AddInt64(&completed, 1)
		}
		for i := 0; i < before; i++ {
			if err := s.Submit(short); err != nil {
				t.Fatal(err)
			}
		}
		if err := s.Submit(func() {
			spinFor(longWork)
			atomic.AddInt64(&completed, 1)
		}); err != nil {
			t.Fatal(err)
		}
		for i := 0; i < shortTasks-before; i++ {
			if err := s.Submit(short); err != nil {
				t.Fatal(err)
			}
		}
		s.Wait()
		if got := atomic.LoadInt64(&completed); got != shortTasks+1 {
			t.Fatalf("attempt %d: completed %d tasks, want %d", attempt, got, shortTasks+1)
		}
		rep := trace.Analyze(s.Trace())
		s.Close()
		steals += rep.Totals.StealSuccesses
		attemptsCount += rep.Totals.StealAttempts
		worst = fmt.Sprintf("attempt %d (long task at position %d): steals=%d/%d migrations=%d wall=%.1fms",
			attempt, before+1, rep.Totals.StealSuccesses, rep.Totals.StealAttempts,
			rep.Totals.MigrationsIn, float64(rep.WallNanos)/1e6)
		t.Log(worst)
	}
	if steals == 0 {
		t.Fatalf("a worker held queued work while stuck on a %v task and nobody stole it in %d attempts (%d probes); last: %s",
			longWork, attempts, attemptsCount, worst)
	}
}

// TestBlockingHandoff proves the cooperative path: a task that declares it is
// about to wait hands its local queue over, and the pool keeps making progress
// instead of deadlocking on work only the blocked worker could have run.
func TestBlockingHandoff(t *testing.T) {
	t.Run("every_p_blocked_still_makes_progress", func(t *testing.T) {
		// One P, so the only worker blocks with children queued behind it. The
		// scheduler must grow a transient helper or this test times out.
		s := New(WithProcs(1), WithTracing(true), WithMaxHelpers(2), WithTraceCapacity(1<<16))
		const children = 12
		var ran int64
		finished := make(chan struct{})
		if err := s.Submit(func() {
			for i := 0; i < children; i++ {
				if err := s.Submit(func() { atomic.AddInt64(&ran, 1) }); err != nil {
					t.Errorf("child Submit: %v", err)
					return
				}
			}
			s.Blocking(func() {
				deadline := time.Now().Add(4 * time.Second)
				for atomic.LoadInt64(&ran) < children {
					if time.Now().After(deadline) {
						return // let the timeout assertion below fail the test
					}
					time.Sleep(20 * time.Microsecond)
				}
			})
			close(finished)
		}); err != nil {
			t.Fatal(err)
		}
		select {
		case <-finished:
		case <-time.After(6 * time.Second):
			t.Fatalf("pool deadlocked with every P blocked; stats=%+v", s.Stats())
		}
		s.Wait()
		if got := atomic.LoadInt64(&ran); got != children {
			t.Fatalf("%d of %d queued children ran", got, children)
		}
		var helperRuns int64
		for _, ev := range s.Trace().Events {
			if ev.Type == trace.Execute && ev.P != 0 {
				helperRuns++
			}
		}
		if helperRuns == 0 {
			t.Fatal("no helper worker executed the work queued behind the blocked P")
		}
		s.Close()
	})

	t.Run("local_queue_is_handed_off_and_migrates", func(t *testing.T) {
		// A worker's local queue is only non-empty when it has claimed more
		// than it has executed, which happens mid-batch. Blockers are spread
		// through the submission stream so that at least one of them is claimed
		// in the middle of a batch with older tasks still queued behind it.
		//
		// One P is deliberate: with nobody else to reclaim it, every task the
		// blocker hands off must be executed by the transient helper, so the
		// migration count is a direct check that the handoff really moved work
		// rather than dropping it.
		const (
			total    = 120
			blockers = 3
			interval = total / (blockers + 1)
		)
		// Four helper slots for three blockers: every blocker can be covered by
		// its own executor, so no task has to wait for another to unblock.
		s := New(WithProcs(1), WithTracing(true), WithMaxHelpers(4), WithTraceCapacity(1<<16))
		var completed int64
		for i := 0; i < total; i++ {
			if i%interval == interval/2 {
				if err := s.Submit(func() {
					// The blocker waits on a child the scheduler cannot see,
					// which is what forces a helper to be started: without one
					// the child could never run and this task would wait
					// forever.
					child := make(chan struct{})
					if err := s.Submit(func() { close(child) }); err != nil {
						t.Errorf("child Submit: %v", err)
						return
					}
					s.Blocking(func() {
						select {
						case <-child:
						case <-time.After(4 * time.Second):
						}
					})
					atomic.AddInt64(&completed, 1)
				}); err != nil {
					t.Fatal(err)
				}
				continue
			}
			if err := s.Submit(func() {
				spinFor(200 * time.Microsecond)
				atomic.AddInt64(&completed, 1)
			}); err != nil {
				t.Fatal(err)
			}
		}
		s.Wait()
		if got := atomic.LoadInt64(&completed); got != total {
			t.Fatalf("completed %d tasks, want %d", got, total)
		}

		rec := s.Trace()
		var handedTotal int64
		for _, ev := range rec.Events {
			if ev.Type == trace.Block {
				handedTotal += ev.Aux
			}
		}
		var migratedFromP0 int64
		for _, ev := range rec.Events {
			if ev.Type == trace.Migrate && ev.Aux == 0 {
				migratedFromP0++
			}
		}
		t.Logf("handed off %d tasks, %d of them executed off the blocked P", handedTotal, migratedFromP0)
		if handedTotal == 0 {
			t.Fatal("no blocker had a local queue to hand off: the handoff path was never exercised")
		}
		// Not every handed-off task has to migrate: once the blocker wakes it
		// can claim its own work back, and re-claiming is correctly not a
		// migration. What must be true is that the handoff really published
		// work to other workers rather than dropping it.
		if migratedFromP0 == 0 {
			t.Fatalf("P0 handed off %d tasks and not one was executed by another worker", handedTotal)
		}
		s.Close()
	})
}

// TestBlockingFromNonWorkerIsSafe covers the API edge: Blocking may be called
// by a goroutine the scheduler does not own, and it must simply run fn.
func TestBlockingFromNonWorkerIsSafe(t *testing.T) {
	s := New(WithProcs(2), WithTracing(true))
	ran := false
	s.Blocking(func() { ran = true })
	if !ran {
		t.Fatal("Blocking did not run its function")
	}
	var blocked, unblocked int64
	rec := s.Trace()
	for _, ev := range rec.Events {
		switch ev.Type {
		case trace.Block:
			blocked++
		case trace.Unblock:
			unblocked++
		}
	}
	if blocked != 0 || unblocked != 0 {
		t.Fatalf("a non-worker Blocking call recorded %d/%d block events, want 0/0", blocked, unblocked)
	}
	s.Close()
}

// TestTraceIsDeterministicWithSingleP pins the trace format as a contract
// rather than a debug dump: with one P, a fixed seed and a fully determined
// workload, two runs must produce the same sequence of decisions.
//
// Timestamps, task durations and queue wait times are excluded because they are
// physical measurements and differ every run; everything that describes a
// decision (which P, which task, which victim, which depth) must match exactly.
func TestTraceIsDeterministicWithSingleP(t *testing.T) {
	run := func(t *testing.T) []trace.Event {
		t.Helper()
		s := New(WithProcs(1), WithTracing(true), WithSeed(12345), WithTraceCapacity(1<<16))
		if err := s.Submit(func() {
			for i := 0; i < 50; i++ {
				n := i
				if err := s.Submit(func() { _ = n * n }); err != nil {
					t.Errorf("child Submit: %v", err)
					return
				}
			}
		}); err != nil {
			t.Fatal(err)
		}
		s.Wait()
		// Close only after the worker has actually parked, so the shutdown
		// events are part of the deterministic sequence rather than a race
		// between Close and the worker's last loop iteration.
		waitFor(t, "worker to park", 5*time.Second, func() bool { return s.Stats().Parked == 1 })
		s.Close()
		return s.Trace().Events
	}

	normalize := func(events []trace.Event) []trace.Event {
		out := make([]trace.Event, len(events))
		copy(out, events)
		for i := range out {
			out[i].Ts = 0
			switch out[i].Type {
			case trace.Execute, trace.Unpark, trace.Unblock:
				// Durations and waits are measurements, not decisions.
				out[i].Aux, out[i].Aux2 = 0, 0
			}
		}
		return out
	}

	first := run(t)
	second := run(t)
	if len(first) == 0 {
		t.Fatal("no events were recorded")
	}
	a, b := normalize(first), normalize(second)
	if len(a) != len(b) {
		t.Fatalf("event count differs between runs: %d vs %d", len(a), len(b))
	}
	for i := range a {
		if a[i] != b[i] {
			t.Fatalf("event %d differs between identical runs:\n run1 %v\n run2 %v", i, a[i], b[i])
		}
	}
	// Sanity-check the shape so the test cannot pass on an empty trace.
	counts := map[trace.EventType]int{}
	for _, ev := range a {
		counts[ev.Type]++
	}
	if counts[trace.Submit] != 51 || counts[trace.Execute] != 51 {
		t.Fatalf("expected 51 submits and 51 executions, got %v", counts)
	}
	if counts[trace.Park] != 1 || counts[trace.Unpark] != 1 {
		t.Fatalf("expected exactly one park/unpark pair, got %v", counts)
	}
}

func procName(procs int) string {
	switch procs {
	case 1:
		return "P1"
	case 2:
		return "P2"
	case 4:
		return "P4"
	case 8:
		return "P8"
	default:
		return "P" + string(rune('0'+procs%10))
	}
}
