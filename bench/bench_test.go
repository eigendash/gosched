package bench

import (
	"runtime"
	"testing"

	"github.com/eigendash/gosched/trace"
)

func benchImpl(b *testing.B, implName, shapeName string) {
	b.Helper()
	impl, ok := ImplByName(implName)
	if !ok {
		b.Fatalf("unknown implementation %q", implName)
	}
	shape, ok := ShapeNamed(shapeName)
	if !ok {
		b.Fatalf("unknown workload %q", shapeName)
	}
	procs := runtime.GOMAXPROCS(0)
	var tasks, wallNs int64
	for i := 0; i < b.N; i++ {
		res, _ := RunOne(impl, shape, procs, 0, false)
		tasks += int64(res.Tasks)
		wallNs += res.WallNs
	}
	if tasks == 0 {
		b.Fatal("no tasks ran")
	}
	b.ReportMetric(float64(wallNs)/float64(tasks), "ns/task")
	b.ReportMetric(float64(tasks)/(float64(wallNs)/1e9), "tasks/sec")
}

// The benchmark matrix mirrors the harness: both baselines and the scheduler
// across the four workloads, plus the untraced scheduler so the cost of
// instrumentation can be separated from the cost of scheduling.

func BenchmarkNaiveUniform(b *testing.B)        { benchImpl(b, "naive", "uniform") }
func BenchmarkNaiveForkJoin(b *testing.B)       { benchImpl(b, "naive", "forkjoin") }
func BenchmarkNaiveSkewed(b *testing.B)         { benchImpl(b, "naive", "skewed") }
func BenchmarkNaiveBlocking(b *testing.B)       { benchImpl(b, "naive", "blocking") }
func BenchmarkPoolUniform(b *testing.B)         { benchImpl(b, "pool", "uniform") }
func BenchmarkPoolForkJoin(b *testing.B)        { benchImpl(b, "pool", "forkjoin") }
func BenchmarkPoolSkewed(b *testing.B)          { benchImpl(b, "pool", "skewed") }
func BenchmarkPoolBlocking(b *testing.B)        { benchImpl(b, "pool", "blocking") }
func BenchmarkSchedUniform(b *testing.B)        { benchImpl(b, "sched", "uniform") }
func BenchmarkSchedForkJoin(b *testing.B)       { benchImpl(b, "sched", "forkjoin") }
func BenchmarkSchedSkewed(b *testing.B)         { benchImpl(b, "sched", "skewed") }
func BenchmarkSchedBlocking(b *testing.B)       { benchImpl(b, "sched", "blocking") }
func BenchmarkSchedNoTraceUniform(b *testing.B) { benchImpl(b, "sched-notrace", "uniform") }
func BenchmarkSchedNoTraceSkewed(b *testing.B)  { benchImpl(b, "sched-notrace", "skewed") }

// BenchmarkTracerRecord isolates the cost of the instrumentation itself: one
// event append and one exact-counter update, with and without the clock read
// that stamps the event. It is the number that explains the tracing overhead
// measured on the end-to-end workloads.
func BenchmarkTracerRecord(b *testing.B) {
	for _, withClock := range []bool{false, true} {
		name := "without_clock"
		if withClock {
			name = "with_clock"
		}
		b.Run(name, func(b *testing.B) {
			tr := trace.New(2, 0, 1<<20)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				ts := int64(0)
				if withClock {
					ts = tr.Now()
				}
				tr.StealFail(0, 1, ts)
			}
		})
	}
}
