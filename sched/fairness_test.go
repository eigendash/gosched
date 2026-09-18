package sched

import (
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

// TestStealFairnessUnderSkew is the fairness claim made concrete: under a
// workload where a few long tasks pin most of the pool while thousands of short
// tasks are queued, stealing must actually happen (a success rate above a
// floor) and no worker may be starved (every worker's busy time is a
// meaningful fraction of the wall clock).
//
// The floors are deliberately low. A high threshold would only pass on an idle
// machine with the exact topology it was tuned on; a low one still fails if
// stealing stops working entirely, which is the regression this guards.
func TestStealFairnessUnderSkew(t *testing.T) {
	const (
		longTasks  = 16
		longWork   = 4 * time.Millisecond
		shortTasks = 2400
		shortWork  = 200 * time.Microsecond
	)
	for _, procs := range []int{4, 8} {
		t.Run(procName(procs), func(t *testing.T) {
			s := New(WithProcs(procs), WithSeed(7), WithTraceCapacity(1<<20))
			var completed int64

			// Long and short tasks are interleaved so that the queue cannot be
			// partitioned by kind: a worker that claims a batch gets both.
			var wgSubmit sync.WaitGroup
			wgSubmit.Add(1)
			go func() {
				defer wgSubmit.Done()
				for i := 0; i < longTasks; i++ {
					if err := s.Submit(func() {
						spinFor(longWork)
						atomic.AddInt64(&completed, 1)
					}); err != nil {
						t.Errorf("Submit: %v", err)
						return
					}
					for j := 0; j < shortTasks/longTasks; j++ {
						if err := s.Submit(func() {
							spinFor(shortWork)
							atomic.AddInt64(&completed, 1)
						}); err != nil {
							t.Errorf("Submit: %v", err)
							return
						}
					}
				}
			}()
			wgSubmit.Wait()
			s.Wait()

			want := int64(longTasks + shortTasks)
			if got := atomic.LoadInt64(&completed); got != want {
				t.Fatalf("completed %d tasks, want %d", got, want)
			}

			rec := s.Trace()
			rep := trace.Analyze(rec)
			share := 1.0
			if rep.MaxUtilization > 0 {
				share = rep.MinUtilization / rep.MaxUtilization
			}
			t.Logf("procs=%d wall=%.1fms steals=%d/%d (%.1f%%) migrations=%d util mean=%.0f%% min=%.0f%% max=%.0f%% work-share=%.2f",
				procs, float64(rep.WallNanos)/1e6,
				rep.Totals.StealSuccesses, rep.Totals.StealAttempts, rep.StealSuccessRate*100,
				rep.Totals.MigrationsIn,
				rep.MeanUtilization*100, rep.MinUtilization*100, rep.MaxUtilization*100, share)

			if rep.Totals.Executed != want {
				t.Fatalf("trace saw %d executions, want %d", rep.Totals.Executed, want)
			}
			if rep.Totals.StealAttempts == 0 {
				t.Fatal("no steal was ever attempted: the pool never had an idle worker")
			}
			if got := rep.Totals.StealSuccesses; got == 0 {
				t.Fatal("no steal ever succeeded under a skewed workload")
			}
			if got := rep.StealSuccessRate; got < 0.01 {
				t.Fatalf("steal success rate %.3f%% is below the 1%% floor: work is not being rebalanced", got*100)
			}
			// Starvation check. It compares the least-busy worker's measured
			// work against the busiest one rather than against the wall clock:
			// both are wall-time-normalised, so the ratio is "share of the
			// work", which survives the slowdown of running under -race, while
			// an absolute utilisation floor would not.
			if share < 0.35 {
				t.Fatalf("least-used worker did only %.0f%% of the busiest worker's work: a worker was starved (min=%.1f%% max=%.1f%%)",
					share*100, rep.MinUtilization*100, rep.MaxUtilization*100)
			}
			s.Close()
		})
	}
}

// TestQueuedWorkIsStealableWhileOwnerIsBusy covers the structure that makes
// stealing worth having: a worker holding queued tasks must be able to lose
// them to an idle peer even though it is not cooperating in any way (it never
// blocks, never yields, and the scheduler is never told it is busy).
//
// The imbalance is structural rather than lucky: long tasks are interleaved
// with a stream of short ones, so whichever workers claim the long ones finish
// their short work long after the others have run dry.
func TestQueuedWorkIsStealableWhileOwnerIsBusy(t *testing.T) {
	const (
		procs     = 4
		longTasks = 2
		longWork  = 30 * time.Millisecond
		shortRuns = 600
		shortWork = 100 * time.Microsecond
	)
	s := New(WithProcs(procs), WithSeed(3), WithTraceCapacity(1<<20))
	var completed int64
	perLong := shortRuns / longTasks
	for i := 0; i < longTasks; i++ {
		if err := s.Submit(func() {
			spinFor(longWork)
			atomic.AddInt64(&completed, 1)
		}); err != nil {
			t.Fatal(err)
		}
		for j := 0; j < perLong; j++ {
			if err := s.Submit(func() {
				spinFor(shortWork)
				atomic.AddInt64(&completed, 1)
			}); err != nil {
				t.Fatal(err)
			}
		}
	}
	s.Wait()

	if got := atomic.LoadInt64(&completed); got != longTasks+shortRuns {
		t.Fatalf("completed %d tasks, want %d", got, longTasks+shortRuns)
	}
	rep := trace.Analyze(s.Trace())
	t.Logf("steals=%d/%d migrations=%d wall=%.1fms util min=%.0f%%",
		rep.Totals.StealSuccesses, rep.Totals.StealAttempts, rep.Totals.MigrationsIn,
		float64(rep.WallNanos)/1e6, rep.MinUtilization*100)
	if rep.Totals.StealSuccesses == 0 {
		t.Fatalf("workers holding queued work while stuck on %v tasks lost nothing to idle peers", longWork)
	}
	s.Close()
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
