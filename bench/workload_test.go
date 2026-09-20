package bench

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// countingSpawner wraps a Spawner and counts executions, which is how the tests
// check that a Shape's Tasks field - the throughput denominator - is the truth.
type countingSpawner struct {
	Spawner
	exec atomic.Int64
}

func (c *countingSpawner) Submit(t Task) {
	c.exec.Add(1)
	c.Spawner.Submit(func() { t() })
}

// TestShapesTaskCountsAreExact is a guard on the benchmark's own arithmetic: if
// a shape reported fewer tasks than it runs, every throughput number derived
// from it would be wrong in its favour.
func TestShapesTaskCountsAreExact(t *testing.T) {
	for _, shape := range Shapes() {
		t.Run(shape.Name, func(t *testing.T) {
			cs := &countingSpawner{Spawner: NewNaive()}
			shape.Run(cs, nil)
			cs.Wait()
			cs.Close()
			if got := cs.exec.Load(); got != int64(shape.Tasks) {
				t.Fatalf("shape %s ran %d tasks but reports Tasks=%d", shape.Name, got, shape.Tasks)
			}
		})
	}
}

// TestSpawnersRunEveryTaskOnce checks all three implementations against the
// same contract before any of them is used as a baseline. A baseline that
// silently dropped or duplicated work would make the scheduler look good for
// the wrong reason.
func TestSpawnersRunEveryTaskOnce(t *testing.T) {
	for _, im := range Impls() {
		t.Run(im.Name, func(t *testing.T) {
			sp := im.New(4)
			const n = 2000
			var ran int64
			for i := 0; i < n; i++ {
				sp.Submit(func() { atomic.AddInt64(&ran, 1) })
			}
			sp.Wait()
			sp.Close()
			if got := atomic.LoadInt64(&ran); got != n {
				t.Fatalf("%s ran %d tasks, want %d", im.Name, got, n)
			}
		})
	}
}

// TestForkJoinOnEveryImpl exercises the shape that can deadlock a fixed pool:
// workers submit children into the same queue they are draining. All three
// implementations must finish it, which is also why baseline B uses a buffered
// channel.
func TestForkJoinOnEveryImpl(t *testing.T) {
	const depth = 8
	expected := int64(1)<<(depth+1) - 1
	for _, im := range Impls() {
		t.Run(im.Name, func(t *testing.T) {
			sp := im.New(4)
			var ran int64
			var spawn func(d int)
			spawn = func(d int) {
				sp.Submit(func() {
					atomic.AddInt64(&ran, 1)
					if d > 0 {
						spawn(d - 1)
						spawn(d - 1)
					}
				})
			}
			done := make(chan struct{})
			go func() {
				spawn(depth)
				sp.Wait()
				close(done)
			}()
			select {
			case <-done:
			case <-time.After(30 * time.Second):
				t.Fatalf("%s deadlocked on a fork-join workload", im.Name)
			}
			sp.Close()
			if got := atomic.LoadInt64(&ran); got != expected {
				t.Fatalf("%s ran %d tasks, want %d", im.Name, got, expected)
			}
		})
	}
}

// TestBlockingRunnerLetsBlockedTasksComplete makes sure the blocking workload
// is answerable on every implementation, including the scheduler path that
// hands work off when a task declares a wait.
func TestBlockingRunnerLetsBlockedTasksComplete(t *testing.T) {
	shape := Blocking(24, time.Millisecond)
	cs := &countingSpawner{Spawner: NewNaive()}
	shape.Run(cs, nil)
	cs.Wait()
	cs.Close()
	if got := cs.exec.Load(); got != int64(shape.Tasks) {
		t.Fatalf("blocking shape ran %d tasks, want %d", got, shape.Tasks)
	}

	// And with the scheduler, where blocking is cooperative.
	impl, _ := ImplByName("sched")
	sp := impl.New(2)
	done := make(chan struct{})
	go func() {
		shape.Run(sp, nil)
		sp.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("scheduler deadlocked on the blocking workload")
	}
	sp.Close()
}

// TestLatencyPercentiles checks the recorder itself, since the tail numbers in
// the results files come from it.
func TestLatencyPercentiles(t *testing.T) {
	l := NewLatency(100)
	for i := 0; i < 100; i++ {
		l.Record(i, time.Duration(i+1)*time.Microsecond)
	}
	p50, p99, max := l.Percentiles()
	if p50 != 51*time.Microsecond {
		t.Errorf("p50 = %v, want 51us", p50)
	}
	if p99 != 100*time.Microsecond {
		t.Errorf("p99 = %v, want 100us", p99)
	}
	if max != 100*time.Microsecond {
		t.Errorf("max = %v, want 100us", max)
	}
	var zero *Latency
	if a, b, c := zero.Percentiles(); a != 0 || b != 0 || c != 0 {
		t.Errorf("nil latency recorder returned %v/%v/%v, want zeroes", a, b, c)
	}
}

// TestMedianOfResults checks the aggregation the summary files use: a single
// descheduled run must not move the reported number.
func TestMedianOfResults(t *testing.T) {
	results := []Result{
		{Impl: "x", Wall: 10 * time.Millisecond},
		{Impl: "x", Wall: 9 * time.Millisecond},
		{Impl: "x", Wall: 100 * time.Millisecond},
		{Impl: "x", Wall: 11 * time.Millisecond},
		{Impl: "x", Wall: 10 * time.Millisecond},
	}
	got := Median(results)
	if got.Wall != 10*time.Millisecond {
		t.Fatalf("median wall = %v, want 10ms", got.Wall)
	}
	if m := Median(nil); m.Wall != 0 {
		t.Fatalf("median of nothing = %v", m.Wall)
	}
}

// TestRunOneIsReproducibleInShape guards against a shape whose Task count or
// completion depends on the implementation: the same shape run twice on the
// naive runner must produce the same task count.
func TestRunOneIsReproducibleInShape(t *testing.T) {
	shape := Uniform(50, 10*time.Microsecond)
	impl, _ := ImplByName("naive")
	first, _ := RunOne(impl, shape, 2, 0, true)
	second, _ := RunOne(impl, shape, 2, 1, true)
	if first.Tasks != second.Tasks {
		t.Fatalf("task count changed between runs: %d vs %d", first.Tasks, second.Tasks)
	}
	if first.P50 <= 0 || first.P99 <= 0 || first.Max <= 0 {
		t.Fatalf("latency recorder produced %v/%v/%v", first.P50, first.P99, first.Max)
	}
	if first.P50 > first.Max || first.P99 > first.Max {
		t.Fatalf("percentiles out of order: p50=%v p99=%v max=%v", first.P50, first.P99, first.Max)
	}
}

// TestSchedSpawnerIsNotSlowerThanItself is a smoke check that the traced and
// untraced paths both run the same workload to completion, which the overhead
// table depends on.
func TestSchedSpawnerRunsTracedAndUntraced(t *testing.T) {
	shape := Uniform(200, 20*time.Microsecond)
	var wg sync.WaitGroup
	for _, name := range []string{"sched", "sched-notrace"} {
		impl, _ := ImplByName(name)
		wg.Add(1)
		go func(name string) {
			defer wg.Done()
			res, _ := RunOne(impl, shape, 2, 0, false)
			if res.Tasks != 200 {
				t.Errorf("%s reported %d tasks, want 200", name, res.Tasks)
			}
		}(name)
	}
	wg.Wait()
}
