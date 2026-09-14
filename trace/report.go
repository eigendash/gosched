package trace

import (
	"fmt"
	"io"
	"strings"
)

// Report is the aggregate view of a recording: the numbers a reviewer needs to
// explain why a run behaved the way it did.
//
// Every field here comes from the tracer's exact counters rather than from the
// event stream, except where marked. That distinction matters because the event
// buffer is lossy under load; a report that quietly derived "steals succeeded"
// by counting events would under-report precisely when the system was busiest.
type Report struct {
	Procs     int
	Capacity  int
	WallNanos int64

	EventsRecorded int64
	EventsDropped  int64
	DropFraction   float64

	PerP    []Counters
	Helpers Counters
	Sink    Counters
	Totals  Counters

	StealSuccessRate float64
	MigrationRate    float64
	MeanQueueDepth   float64
	MaxQueueDepth    int64
	MeanUtilization  float64
	MinUtilization   float64
	MaxUtilization   float64
}

// Analyze folds a recording into a report.
func Analyze(rec *Recording) *Report {
	r := &Report{
		Procs:          rec.Procs,
		Capacity:       rec.Capacity,
		WallNanos:      rec.WallNanos,
		EventsRecorded: int64(len(rec.Events)),
		EventsDropped:  rec.Dropped,
		PerP:           make([]Counters, rec.Procs),
	}
	total := int64(len(rec.Events)) + rec.Dropped
	if total > 0 {
		r.DropFraction = float64(rec.Dropped) / float64(total)
	}
	for i, c := range rec.PerP {
		switch {
		case i < rec.Procs:
			r.PerP[i] = c
			addCounters(&r.Totals, &c)
		case i < len(rec.PerP)-1:
			addCounters(&r.Helpers, &c)
		default:
			r.Sink = c
		}
	}
	addCounters(&r.Totals, &r.Helpers)
	// The sink ring only ever sees submits; folding it in keeps "tasks
	// submitted" complete regardless of which goroutine did the submitting.
	addCounters(&r.Totals, &r.Sink)

	if r.Totals.StealAttempts > 0 {
		r.StealSuccessRate = float64(r.Totals.StealSuccesses) / float64(r.Totals.StealAttempts)
	}
	if r.Totals.Executed > 0 {
		r.MigrationRate = float64(r.Totals.MigrationsIn) / float64(r.Totals.Executed)
	}
	if r.Totals.QueueDepthCount > 0 {
		r.MeanQueueDepth = float64(r.Totals.QueueDepthSum) / float64(r.Totals.QueueDepthCount)
	}
	r.MaxQueueDepth = r.Totals.QueueDepthMax

	if rec.WallNanos > 0 {
		r.MinUtilization = 1
		for i := range r.PerP {
			util := float64(r.PerP[i].ExecutedNanos) / float64(rec.WallNanos)
			if util < r.MinUtilization {
				r.MinUtilization = util
			}
			if util > r.MaxUtilization {
				r.MaxUtilization = util
			}
			r.MeanUtilization += util
		}
		if len(r.PerP) > 0 {
			r.MeanUtilization /= float64(len(r.PerP))
		}
	}
	return r
}

func addCounters(dst, src *Counters) {
	dst.Submits += src.Submits
	dst.Executed += src.Executed
	dst.ExecutedNanos += src.ExecutedNanos
	dst.StealAttempts += src.StealAttempts
	dst.StealSuccesses += src.StealSuccesses
	dst.StolenIn += src.StolenIn
	dst.StolenOut += src.StolenOut
	dst.ProbedEmpty += src.ProbedEmpty
	dst.MigrationsIn += src.MigrationsIn
	dst.Parked += src.Parked
	dst.Unparked += src.Unparked
	dst.Blocked += src.Blocked
	dst.Unblocked += src.Unblocked
	dst.BlockNanos += src.BlockNanos
	dst.QueueDepthSum += src.QueueDepthSum
	dst.QueueDepthMax = max64(dst.QueueDepthMax, src.QueueDepthMax)
	dst.QueueDepthCount += src.QueueDepthCount
	dst.Dropped += src.Dropped
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func millis(ns int64) float64 { return float64(ns) / 1e6 }

// WriteText renders the report as a fixed-width table that stays readable in a
// terminal and in a diff.
func (r *Report) WriteText(w io.Writer) error {
	p := func(format string, args ...any) error {
		_, err := fmt.Fprintf(w, format, args...)
		return err
	}
	if err := p("gosched trace report\n====================\n"); err != nil {
		return err
	}
	if err := p("wall time                    : %.3f ms\n", millis(r.WallNanos)); err != nil {
		return err
	}
	if err := p("logical processors (P)       : %d\n", r.Procs); err != nil {
		return err
	}
	if err := p("event capacity per worker    : %d\n", r.Capacity); err != nil {
		return err
	}
	if err := p("events recorded              : %d\n", r.EventsRecorded); err != nil {
		return err
	}
	if err := p("events dropped               : %d (%.4f%% of generated)\n", r.EventsDropped, r.DropFraction*100); err != nil {
		return err
	}
	if err := p("tasks submitted              : %d\n", r.Totals.Submits); err != nil {
		return err
	}
	if err := p("tasks executed               : %d\n", r.Totals.Executed); err != nil {
		return err
	}
	if err := p("execution migrations         : %d (%.2f%% of executions ran off their submitting P)\n",
		r.Totals.MigrationsIn, r.MigrationRate*100); err != nil {
		return err
	}
	if err := p("steal attempts               : %d\n", r.Totals.StealAttempts); err != nil {
		return err
	}
	if err := p("steal successes              : %d (%.2f%% of attempts; %d probes absorbed by empty victims)\n",
		r.Totals.StealSuccesses, r.StealSuccessRate*100, r.Totals.ProbedEmpty); err != nil {
		return err
	}
	if err := p("mean local queue depth       : %.2f\n", r.MeanQueueDepth); err != nil {
		return err
	}
	if err := p("max local queue depth        : %d\n", r.MaxQueueDepth); err != nil {
		return err
	}
	if err := p("park / unpark                : %d / %d\n", r.Totals.Parked, r.Totals.Unparked); err != nil {
		return err
	}
	if err := p("block / unblock              : %d / %d (%.3f ms blocked in total)\n",
		r.Totals.Blocked, r.Totals.Unblocked, millis(r.Totals.BlockNanos)); err != nil {
		return err
	}
	if err := p("worker utilization           : mean %.1f%%, min %.1f%%, max %.1f%% of wall time\n",
		r.MeanUtilization*100, r.MinUtilization*100, r.MaxUtilization*100); err != nil {
		return err
	}

	if err := p("\nper-worker\n"); err != nil {
		return err
	}
	width := 10
	var b strings.Builder
	header := make([]string, 0, r.Procs+2)
	for i := 0; i < r.Procs; i++ {
		header = append(header, fmt.Sprintf("P%d", i))
	}
	header = append(header, "helpers", "external")
	writeRow := func(label string, values []string) error {
		b.Reset()
		fmt.Fprintf(&b, "%-24s", label)
		for _, v := range values {
			fmt.Fprintf(&b, "%*s", width, v)
		}
		_, err := fmt.Fprintln(w, strings.TrimRight(b.String(), " "))
		return err
	}
	rows := []struct {
		label string
		get   func(Counters) string
	}{
		{"executed", func(c Counters) string { return fmt.Sprint(c.Executed) }},
		{"busy ms", func(c Counters) string { return fmt.Sprintf("%.1f", millis(c.ExecutedNanos)) }},
		{"utilization", func(c Counters) string {
			if r.WallNanos == 0 {
				return "0.0%"
			}
			return fmt.Sprintf("%.1f%%", float64(c.ExecutedNanos)/float64(r.WallNanos)*100)
		}},
		{"steal attempts", func(c Counters) string { return fmt.Sprint(c.StealAttempts) }},
		{"steal successes", func(c Counters) string { return fmt.Sprint(c.StealSuccesses) }},
		{"stolen in", func(c Counters) string { return fmt.Sprint(c.StolenIn) }},
		{"stolen out (donated)", func(c Counters) string { return fmt.Sprint(c.StolenOut) }},
		{"probes absorbed", func(c Counters) string { return fmt.Sprint(c.ProbedEmpty) }},
		{"ran off-origin", func(c Counters) string { return fmt.Sprint(c.MigrationsIn) }},
		{"parked", func(c Counters) string { return fmt.Sprint(c.Parked) }},
		{"unparked", func(c Counters) string { return fmt.Sprint(c.Unparked) }},
		{"blocked", func(c Counters) string { return fmt.Sprint(c.Blocked) }},
		{"submitted", func(c Counters) string { return fmt.Sprint(c.Submits) }},
		{"events dropped", func(c Counters) string { return fmt.Sprint(c.Dropped) }},
	}
	if err := writeRow("worker", header); err != nil {
		return err
	}
	for _, row := range rows {
		values := make([]string, 0, r.Procs+2)
		for i := 0; i < r.Procs; i++ {
			values = append(values, row.get(r.PerP[i]))
		}
		values = append(values, row.get(r.Helpers), row.get(r.Sink))
		if err := writeRow(row.label, values); err != nil {
			return err
		}
	}
	return nil
}
