package sched

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// forkJoinTree submits a binary tree of tasks where every task submits its own
// children while it runs. It returns the number of tasks the tree will contain
// and the per-task bookkeeping the assertions read afterwards.
//
// The shape is fully determined by maxDepth, so the expected task count is
// arithmetic rather than "whatever happened": a scheduler that silently drops
// or duplicates work cannot make the numbers agree by accident.
func forkJoinTree(t *testing.T, s *Scheduler, maxDepth int) (expected int64, ran []int32, got, want []int64, dupes *atomic.Int64, lateRuns *atomic.Int64) {
	t.Helper()
	total := int64(1)<<(maxDepth+1) - 1
	ran = make([]int32, total)
	got = make([]int64, total)
	want = make([]int64, total)
	var nextID atomic.Int64
	var duplicateRuns, afterWait atomic.Int64
	dupes = &duplicateRuns
	lateRuns = &afterWait

	// valueFor is a pure function of (id, depth). Any lost, duplicated or
	// mis-parented task shows up as a mismatch.
	valueFor := func(id int64, depth int) int64 { return id*31 + int64(depth) }

	var spawn func(id int64, depth int)
	spawn = func(id int64, depth int) {
		if err := s.Submit(func() {
			if atomic.AddInt32(&ran[id], 1) != 1 {
				duplicateRuns.Add(1)
			}
			if afterWait.Load() != 0 {
				afterWait.Add(1)
			}
			got[id] = valueFor(id, depth)
			if depth+1 <= maxDepth {
				for c := 0; c < 2; c++ {
					cid := nextID.Add(1) - 1
					if int(cid) >= len(want) {
						return
					}
					want[cid] = valueFor(cid, depth+1)
					spawn(cid, depth+1)
				}
			}
		}); err != nil {
			t.Errorf("Submit: %v", err)
		}
	}

	want[0] = valueFor(0, 0)
	nextID.Store(1)
	spawn(0, 0)
	return total, ran, got, want, dupes, lateRuns
}

// TestCorrectnessUnderLoad is the headline correctness test: N tasks, each
// doing verifiable work and submitting children, with a per-task assertion that
// it ran exactly once and produced the right value.
func TestCorrectnessUnderLoad(t *testing.T) {
	for _, traced := range []bool{true, false} {
		name := "traced"
		if !traced {
			name = "untraced"
		}
		t.Run(name, func(t *testing.T) {
			const maxDepth = 12 // 8191 tasks
			s := New(WithProcs(runtime.GOMAXPROCS(0)), WithTracing(traced), WithTraceCapacity(1<<20))
			expected, ran, got, want, dupes, _ := forkJoinTree(t, s, maxDepth)
			s.Wait()

			if n := dupes.Load(); n != 0 {
				t.Fatalf("%d tasks ran more than once", n)
			}
			var executed int64
			for id, n := range ran {
				if n != 1 {
					t.Fatalf("task %d ran %d times, want exactly 1", id, n)
				}
				executed++
				if got[id] != want[id] {
					t.Fatalf("task %d produced %d, want %d", id, got[id], want[id])
				}
			}
			if executed != expected {
				t.Fatalf("executed %d tasks, want %d", executed, expected)
			}
			if p := s.Stats().Pending; p != 0 {
				t.Fatalf("pending = %d after Wait, want 0", p)
			}
			if err := s.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
		})
	}
}

// TestWaitCoversTransitiveClosure checks the property Wait actually promises:
// when it returns, nothing from the transitive closure is still running -- not
// the submitted tasks, not the children they submitted while executing. Tasks
// are deliberately slow enough that a missing increment lets one land after
// Wait returns.
func TestWaitCoversTransitiveClosure(t *testing.T) {
	s := New(WithProcs(4), WithTracing(false))

	var live, completed, startsAfterWait int64
	var done atomic.Bool

	const depth = 6
	var spawn func(d int)
	spawn = func(d int) {
		atomic.AddInt64(&live, 1)
		if err := s.Submit(func() {
			if done.Load() {
				atomic.AddInt64(&startsAfterWait, 1)
			}
			time.Sleep(100 * time.Microsecond)
			if d > 0 {
				spawn(d - 1)
				spawn(d - 1)
			}
			atomic.AddInt64(&completed, 1)
			atomic.AddInt64(&live, -1)
		}); err != nil {
			t.Errorf("Submit: %v", err)
		}
	}
	expected := int64(1)<<(depth+1) - 1
	spawn(depth)
	s.Wait()
	done.Store(true)

	if got := atomic.LoadInt64(&completed); got != expected {
		t.Fatalf("completed %d tasks when Wait returned, want %d", got, expected)
	}
	if got := atomic.LoadInt64(&live); got != 0 {
		t.Fatalf("%d tasks still live when Wait returned, want 0", got)
	}
	// A straggler that the counter logic forgot would show up here.
	time.Sleep(20 * time.Millisecond)
	if got := atomic.LoadInt64(&completed); got != expected {
		t.Fatalf("completed grew to %d after Wait returned", got)
	}
	if got := atomic.LoadInt64(&startsAfterWait); got != 0 {
		t.Fatalf("%d tasks started after Wait returned", got)
	}
	s.Close()
}

// TestNoLostWakeups is the specific race the wakeup protocol exists to prevent:
// several goroutines submit while the workers repeatedly drain to empty and
// park, so every submission races a worker either going to sleep or waking up.
//
// The test also asserts that parking actually happened, so it cannot pass by
// accident on a machine that happened to keep every worker busy.
func TestNoLostWakeups(t *testing.T) {
	const (
		submitters = 8
		rounds     = 24
		perRound   = 5
		procs      = 2
	)
	total := int64(submitters * rounds * perRound)

	s := New(WithProcs(procs), WithTracing(true))
	var executed int64
	var wg sync.WaitGroup
	start := make(chan struct{})
	for g := 0; g < submitters; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			<-start
			for r := 0; r < rounds; r++ {
				for i := 0; i < perRound; i++ {
					if err := s.Submit(func() { atomic.AddInt64(&executed, 1) }); err != nil {
						t.Errorf("Submit: %v", err)
						return
					}
				}
				// Long enough for both workers to run dry and park, which is the
				// window where a lost wakeup would strand these tasks forever.
				time.Sleep(200 * time.Microsecond)
			}
		}(g)
	}
	close(start)
	wg.Wait()
	s.Wait()

	if got := atomic.LoadInt64(&executed); got != total {
		t.Fatalf("executed %d tasks, want %d (tasks were stranded or duplicated)", got, total)
	}
	rec := s.Trace()
	var parked int64
	for _, c := range rec.PerP {
		parked += c.Parked
	}
	if parked == 0 {
		t.Fatal("no worker ever parked: the test did not exercise the wakeup race")
	}
	// Give any phantom wakeup a chance to reveal itself by re-running queued
	// work that does not exist.
	time.Sleep(10 * time.Millisecond)
	if got := atomic.LoadInt64(&executed); got != total {
		t.Fatalf("executed count changed to %d after Wait returned", got)
	}
	s.Close()
}

// TestNoteNeverStrandsASleeper drives the wakeup primitive through both of its
// interesting interleavings: a wake that arrives while the sleeper is running
// (it must be remembered as a token) and a wake that arrives while the sleeper
// is blocked (it must release it). Neither may be lost, because a lost wakeup
// is exactly the bug that parks a worker forever with work still queued.
func TestNoteNeverStrandsASleeper(t *testing.T) {
	t.Run("wake_while_running_is_remembered", func(t *testing.T) {
		n := newNote()
		for i := 0; i < 5; i++ {
			n.wake()
		}
		// Every one of these must return without blocking: tokens accumulated
		// while nobody was asleep are observed by the next sleep.
		n.sleep()
	})

	t.Run("wake_releases_a_blocked_sleeper", func(t *testing.T) {
		for cycle := 0; cycle < 300; cycle++ {
			n := newNote()
			awake := make(chan struct{})
			go func() {
				n.sleep()
				close(awake)
			}()
			// Wait until the sleeper has definitely published itself as asleep,
			// which is the state a waker must not miss.
			deadline := time.Now().Add(2 * time.Second)
			for n.state.Load() != noteSleeping {
				if time.Now().After(deadline) {
					t.Fatalf("cycle %d: sleeper never went to sleep", cycle)
				}
				runtime.Gosched()
			}
			n.wake()
			select {
			case <-awake:
			case <-time.After(2 * time.Second):
				t.Fatalf("cycle %d: wake did not release the sleeper", cycle)
			}
		}
	})

	t.Run("coalesced_wakes_do_not_deadlock", func(t *testing.T) {
		// Wakes that pile up while the sleeper is awake collapse into one
		// token; the note must still be usable afterwards.
		n := newNote()
		var wg sync.WaitGroup
		for i := 0; i < 8; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < 100; j++ {
					n.wake()
				}
			}()
		}
		wg.Wait()
		done := make(chan struct{})
		go func() {
			n.sleep()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("sleeper stuck behind coalesced wakes")
		}
		// After consuming the token the note must return to idle, not stay
		// stuck in a state that swallows the next sleep.
		if got := n.state.Load(); got != noteIdle {
			t.Fatalf("note state after sleep = %d, want idle", got)
		}
	})

	t.Run("concurrent_wake_storm_never_hangs", func(t *testing.T) {
		n := newNote()
		stop := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				n.sleep()
				n.wake() // hand the token on so the loop cannot starve
			}
		}()
		for i := 0; i < 4; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < 20000; j++ {
					n.wake()
				}
			}()
		}
		time.Sleep(100 * time.Millisecond)
		close(stop)
		wg.Wait()
	})
}

// TestSubmitAfterCloseAndNilTask pins the API edge cases that would otherwise
// show up as silent work loss.
func TestSubmitAfterCloseAndNilTask(t *testing.T) {
	s := New(WithProcs(2), WithTracing(false))
	if err := s.Submit(nil); err != ErrNilTask {
		t.Fatalf("Submit(nil) = %v, want ErrNilTask", err)
	}
	var ran int64
	for i := 0; i < 100; i++ {
		if err := s.Submit(func() { atomic.AddInt64(&ran, 1) }); err != nil {
			t.Fatalf("Submit: %v", err)
		}
	}
	s.Wait()
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := s.Submit(func() {}); err != ErrClosed {
		t.Fatalf("Submit after Close = %v, want ErrClosed", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if got := atomic.LoadInt64(&ran); got != 100 {
		t.Fatalf("ran %d tasks, want 100 (Close must drain before stopping)", got)
	}
}
