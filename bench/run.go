package bench

import (
	"fmt"
	"runtime"
	"sort"
	"time"

	"github.com/eigendash/gosched/sched"
	"github.com/eigendash/gosched/trace"
)

// PoolQueue is the shared channel's capacity for baseline B. It is deliberately
// generous: the point of that baseline is lock contention on one channel, not
// starvation, and a small buffer would turn a fork-join workload into a
// deadlock instead of a measurement.
const PoolQueue = 1 << 14

// Impl is one implementation under comparison.
type Impl struct {
	Name string
	New  func(procs int) Spawner
}

// SchedCap is the per-worker trace budget used by the traced scheduler in the
// harness. It is large enough that the example traces are complete, and the
// drop counter makes any overflow visible instead of silent.
const SchedCap = 1 << 16

// Impls is the full comparison set: the two baselines plus the scheduler with
// and without instrumentation. The untraced scheduler is what separates "work
// stealing is slower" from "tracing is slower", which is the question a
// reviewer will actually ask.
func Impls() []Impl {
	return []Impl{
		{Name: "naive", New: func(int) Spawner { return NewNaive() }},
		{Name: "pool", New: func(p int) Spawner { return NewPool(p, PoolQueue) }},
		{Name: "sched", New: func(p int) Spawner {
			return NewSched(sched.New(sched.WithProcs(p), sched.WithTracing(true), sched.WithTraceCapacity(SchedCap)))
		}},
		{Name: "sched-notrace", New: func(p int) Spawner {
			return NewSched(sched.New(sched.WithProcs(p), sched.WithTracing(false)))
		}},
	}
}

// ImplByName looks up an implementation, reporting whether it exists.
func ImplByName(name string) (Impl, bool) {
	for _, im := range Impls() {
		if im.Name == name {
			return im, true
		}
	}
	return Impl{}, false
}

// Sched adapts the work stealing scheduler to the Spawner surface.
type Sched struct {
	s *sched.Scheduler
}

// NewSched wraps an existing scheduler.
func NewSched(s *sched.Scheduler) *Sched { return &Sched{s: s} }

// Submit hands a task to the scheduler. The two Task types are distinct
// named types with identical underlying types, so this is a conversion and not
// a wrapper allocation.
func (s *Sched) Submit(t Task) { _ = s.s.Submit(sched.Task(t)) }

// Wait blocks until the transitive closure of submitted tasks has run.
func (s *Sched) Wait() { s.s.Wait() }

// Close stops the scheduler.
func (s *Sched) Close() { _ = s.s.Close() }

// Block tells the scheduler the caller is about to wait on something external.
func (s *Sched) Block(fn func()) { s.s.Blocking(fn) }

// Trace returns the scheduler's recording. Only meaningful after Close.
func (s *Sched) Trace() *trace.Recording { return s.s.Trace() }

// Spread reports the best, median and worst throughput observed for one cell,
// so the results files can state the measurement noise instead of implying that
// a single run is exact.
type Spread struct {
	Impl, Workload string
	Procs          int
	Min, Median    float64
	Max            float64
	Runs           int
}

// Spreads groups results into per-cell spreads.
func Spreads(all []Result) []Spread {
	type key struct {
		impl, workload string
		procs          int
	}
	groups := map[key][]Result{}
	var order []key
	for _, r := range all {
		k := key{r.Impl, r.Workload, r.Procs}
		if _, ok := groups[k]; !ok {
			order = append(order, k)
		}
		groups[k] = append(groups[k], r)
	}
	out := make([]Spread, 0, len(order))
	for _, k := range order {
		g := groups[k]
		best, worst := g[0].Throughput(), g[0].Throughput()
		for _, r := range g {
			t := r.Throughput()
			if t < best {
				best = t
			}
			if t > worst {
				worst = t
			}
		}
		out = append(out, Spread{
			Impl: k.impl, Workload: k.workload, Procs: k.procs,
			Min: best, Median: Median(g).Throughput(), Max: worst, Runs: len(g),
		})
	}
	return out
}

// Result is one measured run.
type Result struct {
	Impl     string        `json:"impl"`
	Workload string        `json:"workload"`
	Procs    int           `json:"procs"`
	Rep      int           `json:"rep"`
	Tasks    int           `json:"tasks"`
	Wall     time.Duration `json:"-"`
	WallNs   int64         `json:"wall_ns"`
	P50      time.Duration `json:"-"`
	P99      time.Duration `json:"-"`
	Max      time.Duration `json:"-"`

	TraceExecuted   int64 `json:"trace_executed,omitempty"`
	TraceSteals     int64 `json:"trace_steals,omitempty"`
	TraceAttempts   int64 `json:"trace_attempts,omitempty"`
	TraceDropped    int64 `json:"trace_dropped,omitempty"`
	TraceMigrations int64 `json:"trace_migrations,omitempty"`
	TraceHandedOff  int64 `json:"trace_handed_off,omitempty"`
}

// Throughput is tasks per second.
func (r Result) Throughput() float64 {
	if r.Wall <= 0 {
		return 0
	}
	return float64(r.Tasks) / r.Wall.Seconds()
}

// RunOne runs one shape on one implementation at one processor count.
//
// Setup, submission, execution and teardown are all inside the measured window
// so that no implementation gets a cheaper accounting than another: the
// scheduler's workers start on the first submit, and the pool's start in New,
// and both are part of getting the work done.
func RunOne(impl Impl, shape Shape, procs, rep int, withLatency bool) (Result, *sched.Scheduler) {
	runtime.GOMAXPROCS(procs)
	var lat *Latency
	if withLatency {
		lat = NewLatency(shape.Tasks)
	}
	start := time.Now()
	sp := impl.New(procs)
	shape.Run(sp, lat)
	sp.Wait()
	sp.Close()
	wall := time.Since(start)

	res := Result{
		Impl:     impl.Name,
		Workload: shape.Name,
		Procs:    procs,
		Rep:      rep,
		Tasks:    shape.Tasks,
		Wall:     wall,
		WallNs:   wall.Nanoseconds(),
	}
	if withLatency {
		res.P50, res.P99, res.Max = lat.Percentiles()
	}
	if sc, ok := sp.(*Sched); ok {
		rec := sc.Trace()
		rep := trace.Analyze(rec)
		res.TraceExecuted = rep.Totals.Executed
		res.TraceSteals = rep.Totals.StealSuccesses
		res.TraceAttempts = rep.Totals.StealAttempts
		res.TraceDropped = rec.Dropped
		res.TraceMigrations = rep.Totals.MigrationsIn
		res.TraceHandedOff = rep.Totals.HandedOff
		return res, sc.s
	}
	return res, nil
}

// Median collapses repetitions of the same (impl, workload, procs) into the
// median wall time, which is the number the summary reports. Medians rather
// than means because a single descheduled run should not move the result.
func Median(results []Result) Result {
	if len(results) == 0 {
		return Result{}
	}
	sorted := append([]Result(nil), results...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Wall < sorted[j].Wall })
	return sorted[len(sorted)/2]
}

// Shapes is the four-workload comparison set. The sizes are chosen so that one
// run takes tens to a few hundred milliseconds: long enough for scheduling
// behaviour to dominate measurement noise, short enough that a full sweep of
// implementations, workloads and processor counts stays in minutes.
func Shapes() []Shape {
	return []Shape{
		Uniform(6000, 100*time.Microsecond),
		ForkJoin(12, 40*time.Microsecond),
		Skewed(40, 4*time.Millisecond, 4000, 25*time.Microsecond),
		Blocking(800, 250*time.Microsecond),
	}
}

// ShapeNamed returns one shape by name.
func ShapeNamed(name string) (Shape, bool) {
	for _, sh := range Shapes() {
		if sh.Name == name {
			return sh, true
		}
	}
	return Shape{}, false
}

// Machine describes the measurement host. Every number in the README is
// qualified by this, because a speedup curve without a machine is meaningless.
type Machine struct {
	Procs     int
	MaxProcs  int
	GoVersion string
	GOOS      string
	GOARCH    string
	CPU       string
	Physical  int
}

// DescribeMachine collects the host facts the results files record.
func DescribeMachine() Machine {
	return Machine{
		Procs:     runtime.GOMAXPROCS(0),
		MaxProcs:  runtime.NumCPU(),
		GoVersion: runtime.Version(),
		GOOS:      runtime.GOOS,
		GOARCH:    runtime.GOARCH,
	}
}

// Label renders a machine line for the results files.
func (m Machine) Label() string {
	return fmt.Sprintf("go=%s os=%s/%s logical_cores=%d gomaxprocs_default=%d",
		m.GoVersion, m.GOOS, m.GOARCH, m.MaxProcs, m.Procs)
}
