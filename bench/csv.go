package bench

import (
	"encoding/csv"
	"io"
	"strconv"
	"time"
)

// ParseResults reads back the CSV the harness writes.
//
// It exists so the derived tables (medians, speedups, latency percentiles,
// tracing overhead) can be regenerated from the raw runs without re-measuring:
// a reviewer can re-derive every number in the README from bench.csv alone, and
// a claim that cannot be re-derived that way does not belong in the README.
func ParseResults(r io.Reader) ([]Result, error) {
	rd := csv.NewReader(r)
	rows, err := rd.ReadAll()
	if err != nil {
		return nil, err
	}
	if len(rows) < 2 {
		return nil, io.ErrUnexpectedEOF
	}
	idx := map[string]int{}
	for i, name := range rows[0] {
		idx[name] = i
	}
	need := []string{"impl", "workload", "procs", "rep", "tasks", "wall_ns", "p50_ns", "p99_ns", "max_ns",
		"trace_executed", "trace_steal_attempts", "trace_steal_successes", "trace_migrations", "trace_handed_off", "trace_dropped"}
	for _, name := range need {
		if _, ok := idx[name]; !ok {
			return nil, &csvFieldError{name}
		}
	}
	get := func(row []string, name string) string { return row[idx[name]] }
	atoi := func(row []string, name string) int64 {
		v, err := strconv.ParseInt(get(row, name), 10, 64)
		if err != nil {
			return 0
		}
		return v
	}
	atof := func(row []string, name string) float64 {
		v, err := strconv.ParseFloat(get(row, name), 64)
		if err != nil {
			return 0
		}
		return v
	}
	out := make([]Result, 0, len(rows)-1)
	for _, row := range rows[1:] {
		wall := time.Duration(atoi(row, "wall_ns"))
		res := Result{
			Impl:            get(row, "impl"),
			Workload:        get(row, "workload"),
			Procs:           int(atoi(row, "procs")),
			Rep:             int(atoi(row, "rep")),
			Tasks:           int(atoi(row, "tasks")),
			Wall:            wall,
			WallNs:          wall.Nanoseconds(),
			P50:             time.Duration(atoi(row, "p50_ns")),
			P99:             time.Duration(atoi(row, "p99_ns")),
			Max:             time.Duration(atoi(row, "max_ns")),
			TraceExecuted:   atoi(row, "trace_executed"),
			TraceAttempts:   atoi(row, "trace_steal_attempts"),
			TraceSteals:     atoi(row, "trace_steal_successes"),
			TraceMigrations: atoi(row, "trace_migrations"),
			TraceHandedOff:  atoi(row, "trace_handed_off"),
			TraceDropped:    atoi(row, "trace_dropped"),
		}
		_ = atof
		out = append(out, res)
	}
	return out, nil
}

type csvFieldError struct{ field string }

func (e *csvFieldError) Error() string { return "bench: csv is missing column " + e.field }
