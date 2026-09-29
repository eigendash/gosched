// Command gosched-bench runs the comparison matrix and writes the raw
// measurements into results/.
//
// Every number quoted in the README comes from a file this command wrote:
// bench.csv holds every individual run, the summary files hold the medians the
// README quotes, and generating them is a single reproducible command rather
// than a hand-edited table.
package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/eigendash/gosched/bench"
	"github.com/eigendash/gosched/sched"
	"github.com/eigendash/gosched/trace"
)

func main() {
	out := flag.String("out", "results", "directory for the result files")
	reps := flag.Int("reps", 5, "repetitions per (implementation, workload, procs) cell")
	procsCSV := flag.String("procs", "1,2,4,8,10,12", "comma-separated processor counts")
	workloadCSV := flag.String("workloads", "uniform,forkjoin,skewed,blocking", "comma-separated workloads")
	implCSV := flag.String("impls", "naive,pool,sched,sched-notrace", "comma-separated implementations")
	csvPath := flag.String("in", "results/bench.csv", "raw results to re-analyse in -mode analyze")
	mode := flag.String("mode", "matrix", "matrix, analyze, example, overhead, probe or machine")
	flag.Parse()

	if err := os.MkdirAll(*out, 0o755); err != nil {
		fatal(err)
	}
	switch *mode {
	case "matrix":
		matrix(*out, *reps, parseProcs(*procsCSV), splitList(*workloadCSV), splitList(*implCSV))
	case "overhead":
		writeOverhead(*out, parseProcs(*procsCSV))
	case "example":
		writeExampleTrace(*out)
	case "probe":
		writeScalingProbe(*out, parseProcs(*procsCSV))
	case "machine":
		writeMachine(filepath.Join(*out, "machine.txt"), bench.DescribeMachine())
	case "analyze":
		analyze(*out, *csvPath)
	default:
		fatalf("unknown mode %q", *mode)
	}
}

// parseProcs turns "1,2,4" into processor counts, rejecting nonsense early
// rather than letting it produce an unlabelled row in the results.
func parseProcs(s string) []int {
	var out []int
	for _, part := range splitList(s) {
		n, err := strconv.Atoi(part)
		if err != nil || n < 1 {
			fatalf("invalid processor count %q", part)
		}
		out = append(out, n)
	}
	if len(out) == 0 {
		fatalf("no processor counts given")
	}
	return out
}

func splitList(s string) []string {
	parts := strings.Split(s, ",")
	out := parts[:0]
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "gosched-bench:", err)
	os.Exit(1)
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "gosched-bench: "+format+"\n", args...)
	os.Exit(1)
}

func matrix(outDir string, reps int, procs []int, workloadNames, implNames []string) {
	machine := bench.DescribeMachine()
	writeMachine(filepath.Join(outDir, "machine.txt"), machine)

	var impls []bench.Impl
	for _, name := range implNames {
		im, ok := bench.ImplByName(name)
		if !ok {
			fatalf("unknown implementation %q", name)
		}
		impls = append(impls, im)
	}
	var shapes []bench.Shape
	for _, name := range workloadNames {
		sh, ok := bench.ShapeNamed(name)
		if !ok {
			fatalf("unknown workload %q", name)
		}
		shapes = append(shapes, sh)
	}

	// Latency is only meaningful where the shape is about waiting, and only for
	// shapes that submit their tasks directly (a fork-join tree's children are
	// submitted by workers, so there is no single submit time to measure from).
	latencyFor := map[string]bool{"uniform": true, "skewed": true}

	csvFile, err := os.Create(filepath.Join(outDir, "bench.csv"))
	if err != nil {
		fatal(err)
	}
	defer csvFile.Close()
	w := csv.NewWriter(csvFile)
	_ = w.Write([]string{
		"impl", "workload", "procs", "rep", "tasks", "wall_ns", "tasks_per_sec",
		"p50_ns", "p99_ns", "max_ns",
		"trace_executed", "trace_steal_attempts", "trace_steal_successes",
		"trace_migrations", "trace_handed_off", "trace_dropped",
	})

	var all []bench.Result
	// Measurement order matters on a machine with heterogeneous cores and a
	// shared thermal budget: a systematic ordering would hand whichever
	// implementation runs first a cooler CPU. Repetitions therefore interleave
	// the implementations, and each cell is warmed up before it is recorded.
	for _, shape := range shapes {
		for _, p := range procs {
			for _, im := range impls {
				// Warm-up run: costs one cell, keeps the first recorded
				// repetition from paying for cold caches and a cold clock ramp.
				bench.RunOne(im, shape, p, -1, false)
			}
			for rep := 0; rep < reps; rep++ {
				for _, im := range impls {
					res, _ := bench.RunOne(im, shape, p, rep, latencyFor[shape.Name])
					all = append(all, res)
					_ = w.Write([]string{
						res.Impl, res.Workload, strconv.Itoa(res.Procs), strconv.Itoa(res.Rep),
						strconv.Itoa(res.Tasks), strconv.FormatInt(res.WallNs, 10),
						fmt.Sprintf("%.1f", res.Throughput()),
						strconv.FormatInt(res.P50.Nanoseconds(), 10),
						strconv.FormatInt(res.P99.Nanoseconds(), 10),
						strconv.FormatInt(res.Max.Nanoseconds(), 10),
						strconv.FormatInt(res.TraceExecuted, 10),
						strconv.FormatInt(res.TraceAttempts, 10),
						strconv.FormatInt(res.TraceSteals, 10),
						strconv.FormatInt(res.TraceMigrations, 10),
						strconv.FormatInt(res.TraceHandedOff, 10),
						strconv.FormatInt(res.TraceDropped, 10),
					})
					fmt.Fprintf(os.Stderr, "%-14s %-9s P=%-2d rep=%d %8.1f tasks/s\n",
						res.Impl, res.Workload, res.Procs, res.Rep, res.Throughput())
				}
			}
		}
	}
	w.Flush()
	if err := w.Error(); err != nil {
		fatal(err)
	}

	writeSummary(filepath.Join(outDir, "bench-summary.txt"), machine, all, impls, procs, shapes)
	writeVariance(filepath.Join(outDir, "bench-variance.txt"), all)
	writeLatency(filepath.Join(outDir, "skewed-latency.txt"), all, procs)
	writeOverheadFrom(all, filepath.Join(outDir, "trace-overhead.txt"), procs)
	writeExampleTrace(outDir)
}

// writeVariance records how much the numbers moved between repetitions, so the
// README can state the noise floor rather than implying a precision the
// machine does not provide.
func writeVariance(path string, all []bench.Result) {
	f, err := os.Create(path)
	if err != nil {
		fatal(err)
	}
	defer f.Close()
	fmt.Fprintf(f, "run-to-run spread per cell (tasks/sec)\n")
	fmt.Fprintf(f, "=====================================\n")
	fmt.Fprintf(f, "%-14s%-9s%5s%12s%12s%12s%10s\n", "impl", "workload", "P", "min", "median", "max", "spread")
	for _, sp := range bench.Spreads(all) {
		pct := 0.0
		if sp.Median > 0 {
			pct = (sp.Max - sp.Min) / sp.Median * 100
		}
		fmt.Fprintf(f, "%-14s%-9s%5d%12.0f%12.0f%12.0f%9.0f%%\n",
			sp.Impl, sp.Workload, sp.Procs, sp.Min, sp.Median, sp.Max, pct)
	}
}

// analyze re-derives every table the README quotes from the raw runs. It takes
// no measurements, so re-running it is a cheap way to check that a stated number
// still follows from the committed data.
func analyze(outDir, csvPath string) {
	f, err := os.Open(csvPath)
	if err != nil {
		fatal(err)
	}
	defer f.Close()
	all, err := bench.ParseResults(f)
	if err != nil {
		fatal(err)
	}
	impls := uniqueStrings(all, func(r bench.Result) string { return r.Impl })
	workloads := uniqueStrings(all, func(r bench.Result) string { return r.Workload })
	var procs []int
	seen := map[int]bool{}
	for _, r := range all {
		if !seen[r.Procs] {
			seen[r.Procs] = true
			procs = append(procs, r.Procs)
		}
	}
	sort.Ints(procs)

	var implList []bench.Impl
	for _, name := range impls {
		if im, ok := bench.ImplByName(name); ok {
			implList = append(implList, im)
		}
	}
	var shapeList []bench.Shape
	for _, name := range workloads {
		if sh, ok := bench.ShapeNamed(name); ok {
			shapeList = append(shapeList, sh)
		} else {
			shapeList = append(shapeList, bench.Shape{Name: name, Tasks: taskCount(all, name)})
		}
	}

	writeSummary(filepath.Join(outDir, "bench-summary.txt"), bench.DescribeMachine(), all, implList, procs, shapeList)
	writeVariance(filepath.Join(outDir, "bench-variance.txt"), all)
	writeLatency(filepath.Join(outDir, "skewed-latency.txt"), all, procs)
	writeOverheadFrom(all, filepath.Join(outDir, "trace-overhead.txt"), procs)
	fmt.Fprintf(os.Stderr, "gosched-bench: re-derived summary, variance, latency and overhead from %s (%d runs)\n",
		csvPath, len(all))
}

func uniqueStrings(all []bench.Result, key func(bench.Result) string) []string {
	seen := map[string]bool{}
	var out []string
	for _, r := range all {
		k := key(r)
		if !seen[k] {
			seen[k] = true
			out = append(out, k)
		}
	}
	return out
}

func taskCount(all []bench.Result, workload string) int {
	for _, r := range all {
		if r.Workload == workload {
			return r.Tasks
		}
	}
	return 0
}

func writeMachine(path string, m bench.Machine) {
	f, err := os.Create(path)
	if err != nil {
		fatal(err)
	}
	defer f.Close()
	fmt.Fprintf(f, "gosched measurement host\n========================\n")
	fmt.Fprintf(f, "go version      : %s\n", m.GoVersion)
	fmt.Fprintf(f, "os/arch         : %s/%s (%s)\n", m.GOOS, m.GOARCH, osVersion())
	fmt.Fprintf(f, "logical cores   : %d\n", m.MaxProcs)
	fmt.Fprintf(f, "gomaxprocs      : %d\n", m.Procs)
	fmt.Fprintf(f, "host probed at  : %s\n", time.Now().UTC().Format(time.RFC3339))
	fmt.Fprintf(f, "cpu             : %s\n", cpuModel())
	fmt.Fprintf(f, "core topology   : %s\n", coreTopology())
	fmt.Fprintf(f, "processor counts: see bench.csv\n")
}

// medianCell collapses the repetitions of one cell to the median run.
func medianCell(all []bench.Result, impl, workload string, procs int) (bench.Result, bool) {
	var cell []bench.Result
	for _, r := range all {
		if r.Impl == impl && r.Workload == workload && r.Procs == procs {
			cell = append(cell, r)
		}
	}
	if len(cell) == 0 {
		return bench.Result{}, false
	}
	return bench.Median(cell), true
}

func writeSummary(path string, machine bench.Machine, all []bench.Result, impls []bench.Impl, procs []int, shapes []bench.Shape) {
	f, err := os.Create(path)
	if err != nil {
		fatal(err)
	}
	defer f.Close()

	fmt.Fprintf(f, "gosched benchmark summary\n=========================\n")
	fmt.Fprintf(f, "host: %s\n", machine.Label())
	fmt.Fprintf(f, "cpu : %s\n\n", cpuModel())
	fmt.Fprintf(f, "Each cell is the median of the repetitions in bench.csv.\n")
	fmt.Fprintf(f, "\"speedup\" is tasks/sec at P divided by tasks/sec at P=1 for the same implementation.\n\n")

	for _, shape := range shapes {
		fmt.Fprintf(f, "workload: %s (%d tasks per run)\n", shape.Name, shape.Tasks)
		fmt.Fprintf(f, "%-14s", "impl")
		for _, p := range procs {
			fmt.Fprintf(f, "%14s", fmt.Sprintf("P=%d", p))
		}
		fmt.Fprintln(f)
		fmt.Fprintf(f, "%-14s", "throughput")
		for range procs {
			fmt.Fprintf(f, "%14s", "tasks/sec")
		}
		fmt.Fprintln(f)
		for _, im := range impls {
			fmt.Fprintf(f, "%-14s", im.Name)
			for _, p := range procs {
				if r, ok := medianCell(all, im.Name, shape.Name, p); ok {
					fmt.Fprintf(f, "%14.0f", r.Throughput())
				} else {
					fmt.Fprintf(f, "%14s", "-")
				}
			}
			fmt.Fprintln(f)
		}
		fmt.Fprintf(f, "%-14s", "speedup")
		for range procs {
			fmt.Fprintf(f, "%14s", "vs P=1")
		}
		fmt.Fprintln(f)
		for _, im := range impls {
			base, ok := medianCell(all, im.Name, shape.Name, procs[0])
			if !ok || base.Throughput() == 0 {
				continue
			}
			fmt.Fprintf(f, "%-14s", im.Name)
			for _, p := range procs {
				r, ok := medianCell(all, im.Name, shape.Name, p)
				if !ok {
					fmt.Fprintf(f, "%14s", "-")
					continue
				}
				fmt.Fprintf(f, "%13.2fx", r.Throughput()/base.Throughput())
			}
			fmt.Fprintln(f)
		}
		fmt.Fprintln(f)
	}

	// Scheduling telemetry: what the scheduler actually did to get those
	// numbers. These come from the exact per-worker counters, not from the
	// (lossy) event stream.
	fmt.Fprintf(f, "scheduler decisions behind the numbers (median run at each P)\n")
	fmt.Fprintf(f, "%-9s%5s%12s%12s%10s%14s%12s%10s\n",
		"workload", "P", "steals", "attempts", "steal%", "migrations", "handed_off", "dropped")
	for _, shape := range shapes {
		for _, p := range procs {
			r, ok := medianCell(all, "sched", shape.Name, p)
			if !ok {
				continue
			}
			rate := 0.0
			if r.TraceAttempts > 0 {
				rate = float64(r.TraceSteals) / float64(r.TraceAttempts) * 100
			}
			fmt.Fprintf(f, "%-9s%5d%12d%12d%9.1f%%%14d%12d%10d\n",
				shape.Name, p, r.TraceSteals, r.TraceAttempts, rate,
				r.TraceMigrations, r.TraceHandedOff, r.TraceDropped)
		}
	}
}

func writeLatency(path string, all []bench.Result, procs []int) {
	f, err := os.Create(path)
	if err != nil {
		fatal(err)
	}
	defer f.Close()
	fmt.Fprintf(f, "submit-to-completion latency\n============================\n")
	fmt.Fprintf(f, "Latency is measured per task from the submit call to the moment the task\n")
	fmt.Fprintf(f, "returns; for the pool and the scheduler that includes queueing time, while for\n")
	fmt.Fprintf(f, "the naive implementation it is almost pure service time.\n\n")
	fmt.Fprintf(f, "The first block reports the median run of each cell. The second reports how\n")
	fmt.Fprintf(f, "far the p99 moved across the repetitions, because a tail figure from a single\n")
	fmt.Fprintf(f, "run should not be trusted on its own.\n\n")
	for _, workload := range []string{"uniform", "skewed"} {
		fmt.Fprintf(f, "workload: %s (median run per cell)\n", workload)
		fmt.Fprintf(f, "%-14s%5s%14s%14s%14s%14s\n", "impl", "P", "p50", "p99", "max", "tasks/sec")
		for _, im := range []string{"naive", "pool", "sched", "sched-notrace"} {
			for _, p := range procs {
				r, ok := medianCell(all, im, workload, p)
				if !ok {
					continue
				}
				fmt.Fprintf(f, "%-14s%5d%14s%14s%14s%14.0f\n", im, p,
					r.P50.Round(time.Microsecond), r.P99.Round(time.Microsecond),
					r.Max.Round(time.Microsecond), r.Throughput())
			}
		}
		fmt.Fprintln(f)

		fmt.Fprintf(f, "workload: %s (p99 across the repetitions: min, median, max)\n", workload)
		fmt.Fprintf(f, "%-14s%5s%14s%14s%14s%6s\n", "impl", "P", "p99 min", "p99 median", "p99 max", "n")
		for _, im := range []string{"naive", "pool", "sched", "sched-notrace"} {
			for _, p := range procs {
				var cell []bench.Result
				for _, r := range all {
					if r.Impl == im && r.Workload == workload && r.Procs == p {
						cell = append(cell, r)
					}
				}
				if len(cell) == 0 {
					continue
				}
				lo, hi := cell[0].P99, cell[0].P99
				for _, r := range cell {
					if r.P99 < lo {
						lo = r.P99
					}
					if r.P99 > hi {
						hi = r.P99
					}
				}
				med := bench.Median(cell)
				fmt.Fprintf(f, "%-14s%5d%14s%14s%14s%6d\n", im, p,
					lo.Round(time.Microsecond), med.P99.Round(time.Microsecond), hi.Round(time.Microsecond), len(cell))
			}
		}
		fmt.Fprintln(f)
	}
}

func writeOverheadFrom(all []bench.Result, path string, procs []int) {
	f, err := os.Create(path)
	if err != nil {
		fatal(err)
	}
	defer f.Close()
	fmt.Fprintf(f, "tracing overhead (sched vs sched-notrace, median run)\n")
	fmt.Fprintf(f, "=====================================================\n")
	fmt.Fprintf(f, "%-9s%5s%16s%16s%12s%12s\n", "workload", "P", "traced t/s", "untraced t/s", "slowdown", "dropped")
	for _, workload := range []string{"uniform", "forkjoin", "skewed", "blocking"} {
		for _, p := range procs {
			tr, ok := medianCell(all, "sched", workload, p)
			if !ok {
				continue
			}
			un, ok := medianCell(all, "sched-notrace", workload, p)
			if !ok {
				continue
			}
			slow := 0.0
			if un.Throughput() > 0 {
				slow = (un.Throughput() - tr.Throughput()) / un.Throughput() * 100
			}
			fmt.Fprintf(f, "%-9s%5d%16.0f%16.0f%11.1f%%%12d\n",
				workload, p, tr.Throughput(), un.Throughput(), slow, tr.TraceDropped)
		}
	}
}

func writeOverhead(outDir string, procs []int) {
	// Standalone overhead mode: same measurement, but it can be run on its own
	// without re-running the whole matrix.
	shape, _ := bench.ShapeNamed("uniform")
	traced, _ := bench.ImplByName("sched")
	plain, _ := bench.ImplByName("sched-notrace")
	var all []bench.Result
	for _, p := range procs {
		for rep := 0; rep < 5; rep++ {
			a, _ := bench.RunOne(traced, shape, p, rep, false)
			b, _ := bench.RunOne(plain, shape, p, rep, false)
			all = append(all, a, b)
		}
	}
	writeOverheadFrom(all, filepath.Join(outDir, "trace-overhead.txt"), procs)
}

// writeScalingProbe measures whether the harness's own unit of work scales as
// more threads are added.
//
// This exists because the matrix showed aggregate throughput saturating well
// before 12 threads. Two explanations are possible and they need separating:
// the machine's 8 performance + 4 efficiency cores, or the work unit itself. A
// wall-clock spin reads the clock in a tight loop, which is a shared,
// contended read on a many-core machine; an arithmetic-only unit does not. If
// the per-thread rate of the clock probe collapses with thread count while the
// arithmetic probe holds, the harness was measuring clock contention.
func writeScalingProbe(outDir string, procs []int) {
	f, err := os.Create(filepath.Join(outDir, "core-scaling-probe.txt"))
	if err != nil {
		fatal(err)
	}
	defer f.Close()

	fmt.Fprintf(f, "work-unit scaling probe\n")
	fmt.Fprintf(f, "=======================\n")
	fmt.Fprintf(f, "Each row runs the same work on N threads and reports the aggregate rate and\n")
	fmt.Fprintf(f, "the per-thread rate. If a unit scales, the per-thread rate stays flat.\n\n")

	iters := calibrateArithmetic(2 * time.Millisecond)
	fmt.Fprintf(f, "arithmetic unit calibrated to %d iterations (~2ms single-threaded)\n\n", iters)

	fmt.Fprintf(f, "%-9s%14s%14s%14s%14s\n", "threads", "clock agg/s", "clock /thread", "arith agg/s", "arith /thread")
	for _, p := range procs {
		clockAgg, clockPer := probeScaling(p, 60, func() { bench.Spin(2 * time.Millisecond) })
		arithAgg, arithPer := probeScaling(p, 60, func() { arithmetic(iters) })
		fmt.Fprintf(f, "%-9d%14.0f%14.0f%14.0f%14.0f\n", p, clockAgg, clockPer, arithAgg, arithPer)
	}
	fmt.Fprintf(f, "\nA flat per-thread clock rate means the spin primitive is not the limit; a\n")
	fmt.Fprintf(f, "falling one means part of the measured 'scaling' is clock-read contention.\n")
}

// probeScaling runs rounds copies of work on threads goroutines and returns the
// aggregate and per-thread completion rates.
func probeScaling(threads, rounds int, work func()) (aggregate, perThread float64) {
	var wg sync.WaitGroup
	start := time.Now()
	for i := 0; i < threads; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < rounds; j++ {
				work()
			}
		}()
	}
	wg.Wait()
	wall := time.Since(start).Seconds()
	total := float64(threads * rounds)
	if wall <= 0 {
		return 0, 0
	}
	return total / wall, total / wall / float64(threads)
}

// arithmetic is the clock-free work unit: a dependent multiply-add chain that
// the compiler cannot fold away, with no shared state and no time reads.
func arithmetic(iterations int) {
	x := probeSeed
	for i := 0; i < iterations; i++ {
		x = x*6364136223846793005 + 1442695040888963407
	}
	probeSink = x
}

var (
	probeSeed uint64 = 1
	probeSink uint64
)

func calibrateArithmetic(target time.Duration) int {
	const probe = 1 << 20
	start := time.Now()
	arithmetic(probe)
	per := time.Since(start)
	if per <= 0 {
		return probe
	}
	n := int(float64(target) / float64(per) * probe)
	if n < 1 {
		n = 1
	}
	return n
}

// writeExampleTrace produces the committed Chrome Trace Event file and its
// aggregate report. It uses a fixed seed and a single workload so that the
// committed artifact is reproducible rather than a lucky capture.
func writeExampleTrace(outDir string) {
	s := sched.New(
		sched.WithProcs(4),
		sched.WithTracing(true),
		sched.WithSeed(20250907),
		sched.WithTraceCapacity(1<<16),
	)
	fork := bench.ForkJoin(6, 20*time.Microsecond)
	skew := bench.Skewed(6, 2*time.Millisecond, 400, 100*time.Microsecond)
	sp := bench.NewSched(s)
	fork.Run(sp, nil)
	skew.Run(sp, nil)
	sp.Wait()
	sp.Close()

	rec := sp.Trace()
	path := filepath.Join(outDir, "example-trace.json")
	f, err := os.Create(path)
	if err != nil {
		fatal(err)
	}
	if err := trace.WriteChrome(f, rec); err != nil {
		fatal(err)
	}
	if err := f.Close(); err != nil {
		fatal(err)
	}

	// The report is produced by re-importing the file we just wrote, so the
	// committed report also proves the export round-trips.
	back, err := parseTrace(path)
	if err != nil {
		fatal(err)
	}
	rep := trace.Analyze(back)
	rf, err := os.Create(filepath.Join(outDir, "example-trace-report.txt"))
	if err != nil {
		fatal(err)
	}
	defer rf.Close()
	fmt.Fprintf(rf, "source: example-trace.json (re-imported, not the live recording)\n\n")
	if err := rep.WriteText(rf); err != nil {
		fatal(err)
	}

	// A small machine-readable summary so the README claim about the example
	// trace can be checked without re-reading the JSON by hand.
	info, err := os.Create(filepath.Join(outDir, "example-trace-info.txt"))
	if err != nil {
		fatal(err)
	}
	defer info.Close()
	size := int64(0)
	if st, err := os.Stat(path); err == nil {
		size = st.Size()
	}
	fmt.Fprintf(info, "example-trace.json\n")
	fmt.Fprintf(info, "  bytes            : %d\n", size)
	fmt.Fprintf(info, "  events recorded  : %d\n", len(back.Events))
	fmt.Fprintf(info, "  events dropped   : %d\n", back.Dropped)
	fmt.Fprintf(info, "  logical P        : %d\n", back.Procs)
	fmt.Fprintf(info, "  wall (ns)        : %d\n", back.WallNanos)
	fmt.Fprintf(info, "  tasks executed   : %d\n", rep.Totals.Executed)
	fmt.Fprintf(info, "  steal attempts   : %d\n", rep.Totals.StealAttempts)
	fmt.Fprintf(info, "  steal successes  : %d\n", rep.Totals.StealSuccesses)
	counts := map[string]int{}
	for _, ev := range back.Events {
		counts[ev.Type.String()]++
	}
	keys := make([]string, 0, len(counts))
	for k := range counts {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Fprintf(info, "  %-16s : %d\n", k, counts[k])
	}
	fmt.Fprintf(info, "  open it with     : chrome://tracing or https://ui.perfetto.dev\n")
}

func parseTrace(path string) (*trace.Recording, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return trace.ParseChrome(f)
}
