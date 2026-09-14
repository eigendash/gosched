package trace

import (
	"bytes"
	"encoding/json"
	"math"
	"strings"
	"testing"
)

// TestRingOverflowDropsAreCountedExactly pins the lossy policy: a full buffer
// must stop recording rather than corrupt or wrap, and the aggregates must stay
// exact so a report never under-counts because the detail overflowed.
func TestRingOverflowDropsAreCountedExactly(t *testing.T) {
	const capacity = 8
	const generated = 20
	tr := New(1, 0, capacity)
	for i := 0; i < generated; i++ {
		tr.Submit(0, uint64(i+1), 0, int64(i))
	}
	rec := tr.Snapshot()

	if got, want := len(rec.Events), capacity; got != want {
		t.Fatalf("events kept = %d, want %d", got, want)
	}
	if got, want := rec.Dropped, int64(generated-capacity); got != want {
		t.Fatalf("dropped = %d, want %d", got, want)
	}
	if got, want := rec.PerP[0].Submits, int64(generated); got != want {
		t.Fatalf("exact submit counter = %d, want %d", got, want)
	}
	rep := Analyze(rec)
	if got, want := rep.Totals.Submits, int64(generated); got != want {
		t.Fatalf("report submits = %d, want %d", got, want)
	}
	if rep.EventsDropped != int64(generated-capacity) {
		t.Fatalf("report dropped = %d, want %d", rep.EventsDropped, generated-capacity)
	}
	if rep.DropFraction <= 0 || rep.DropFraction >= 1 {
		t.Fatalf("drop fraction = %v, want in (0,1)", rep.DropFraction)
	}
}

// TestDisabledTracerRecordsNothing makes sure the "off" path is really off: no
// events, no counters, nothing for a benchmark to accidentally measure.
func TestDisabledTracerRecordsNothing(t *testing.T) {
	tr := New(2, 0, 16)
	tr.SetEnabled(false)
	if tr.Enabled() {
		t.Fatal("tracer reports enabled after SetEnabled(false)")
	}
	tr.Submit(0, 1, PExternal, 0)
	tr.Execute(0, 1, 1234, 0)
	tr.Park(1, 0, 0)
	rec := tr.Snapshot()
	if len(rec.Events) != 0 {
		t.Fatalf("recorded %d events while disabled", len(rec.Events))
	}
	for i, c := range rec.PerP {
		if c != (Counters{}) {
			t.Fatalf("ring %d has counters %+v while disabled", i, c)
		}
	}
}

// synthetic builds a recording that exercises every event type exactly once per
// worker, with hand-chosen numbers so the assertions are arithmetic rather than
// "something happened".
func synthetic() *Recording {
	tr := New(2, 0, 256)
	tr.Submit(PExternal, 1, PExternal, 0)
	tr.Submit(PExternal, 2, PExternal, 0)

	tr.Migrate(0, 1, PExternal)
	tr.Execute(0, 1, 5_000_000, 0)

	tr.Steal(1, 2, 0, 1)
	tr.Migrate(1, 2, PExternal)
	tr.Execute(1, 2, 3_000_000, 0)
	tr.StealFail(1, 0, 0)
	tr.StealFail(1, 0, 0)
	tr.Park(0, 0, 3)
	tr.Unpark(0)
	tr.Block(0, 0)
	tr.Unblock(0, 1_500_000)
	tr.ObserveQueueDepth(0, 2)
	tr.ObserveQueueDepth(0, 5)
	tr.ObserveQueueDepth(1, 1)
	return tr.Snapshot()
}

// TestChromeExportIsValidTraceEventJSON is the format gate: it parses the file
// as JSON and checks the fields the format requires, rather than only checking
// that encoding/json did not error.
func TestChromeExportIsValidTraceEventJSON(t *testing.T) {
	rec := synthetic()
	var buf bytes.Buffer
	if err := WriteChrome(&buf, rec); err != nil {
		t.Fatalf("WriteChrome: %v", err)
	}

	var doc struct {
		TraceEvents []map[string]any `json:"traceEvents"`
	}
	if err := json.Unmarshal(buf.Bytes(), &doc); err != nil {
		t.Fatalf("trace is not valid JSON: %v", err)
	}
	if len(doc.TraceEvents) == 0 {
		t.Fatal("traceEvents is empty")
	}

	seenNames := map[string]int{}
	lastTs := math.Inf(-1)
	for i, ev := range doc.TraceEvents {
		ph, _ := ev["ph"].(string)
		if ph == "" {
			t.Fatalf("event %d has no phase: %v", i, ev)
		}
		name, _ := ev["name"].(string)
		if name == "" {
			t.Fatalf("event %d has no name: %v", i, ev)
		}
		if _, ok := ev["pid"]; !ok {
			t.Fatalf("event %d has no pid: %v", i, ev)
		}
		if _, ok := ev["tid"]; !ok {
			t.Fatalf("event %d has no tid: %v", i, ev)
		}
		seenNames[name]++
		if ph == "M" {
			if _, ok := ev["args"]; !ok {
				t.Fatalf("metadata event %q has no args", name)
			}
			continue
		}
		ts, ok := ev["ts"].(float64)
		if !ok {
			t.Fatalf("event %d (%s) has no numeric ts: %v", i, name, ev)
		}
		if ts < lastTs {
			t.Fatalf("event %d ts %v went backwards from %v", i, ts, lastTs)
		}
		lastTs = ts
		if ph == "X" {
			if _, ok := ev["dur"].(float64); !ok {
				t.Fatalf("complete event %q has no duration", name)
			}
		}
	}

	for _, want := range []string{"task", "submit", "steal", "steal_fail", "migrate", "park", "unpark", "block", "unblock"} {
		if seenNames[want] == 0 {
			t.Errorf("trace is missing a %q event; saw %v", want, seenNames)
		}
	}
	if seenNames["gosched_stats"] != 1 {
		t.Errorf("expected exactly one gosched_stats metadata event, got %d", seenNames["gosched_stats"])
	}
	if !strings.Contains(buf.String(), `"displayTimeUnit":"ns"`) {
		t.Error("trace does not declare displayTimeUnit")
	}
}

// TestChromeRoundTrip proves the export is complete enough to be the analysis
// input: re-importing it must reproduce the events and the exact counters.
func TestChromeRoundTrip(t *testing.T) {
	rec := synthetic()
	var buf bytes.Buffer
	if err := WriteChrome(&buf, rec); err != nil {
		t.Fatalf("WriteChrome: %v", err)
	}
	back, err := ParseChrome(&buf)
	if err != nil {
		t.Fatalf("ParseChrome: %v", err)
	}
	if back.Procs != rec.Procs || back.Capacity != rec.Capacity || back.Dropped != rec.Dropped {
		t.Fatalf("header mismatch: got procs=%d cap=%d dropped=%d, want procs=%d cap=%d dropped=%d",
			back.Procs, back.Capacity, back.Dropped, rec.Procs, rec.Capacity, rec.Dropped)
	}
	if len(back.Events) != len(rec.Events) {
		t.Fatalf("event count = %d, want %d", len(back.Events), len(rec.Events))
	}
	for i := range rec.Events {
		want, got := rec.Events[i], back.Events[i]
		if got.Type != want.Type || got.P != want.P || got.Task != want.Task || got.Aux != want.Aux || got.Aux2 != want.Aux2 {
			t.Fatalf("event %d mismatch:\n got %v\nwant %v", i, got, want)
		}
		if diff := got.Ts - want.Ts; diff < -1 || diff > 1 {
			t.Fatalf("event %d timestamp drifted by %dns (%v)", i, diff, want)
		}
	}
	if len(back.PerP) != len(rec.PerP) {
		t.Fatalf("per-P counters = %d, want %d", len(back.PerP), len(rec.PerP))
	}
	for i := range rec.PerP {
		if back.PerP[i] != rec.PerP[i] {
			t.Fatalf("ring %d counters mismatch:\n got %+v\nwant %+v", i, back.PerP[i], rec.PerP[i])
		}
	}
}

// TestReportNumbersAreExact checks the analysis arithmetic against the numbers
// synthetic() fed in, so a change in the report cannot silently redefine what
// "steal success rate" or "utilization" means.
func TestReportNumbersAreExact(t *testing.T) {
	rec := synthetic()
	rep := Analyze(rec)

	if rep.Totals.Executed != 2 {
		t.Errorf("executed = %d, want 2", rep.Totals.Executed)
	}
	if rep.Totals.StealAttempts != 3 || rep.Totals.StealSuccesses != 1 {
		t.Errorf("steals = %d/%d, want 1/3", rep.Totals.StealSuccesses, rep.Totals.StealAttempts)
	}
	if want := 1.0 / 3.0; math.Abs(rep.StealSuccessRate-want) > 1e-9 {
		t.Errorf("steal success rate = %v, want %v", rep.StealSuccessRate, want)
	}
	if rep.Totals.StolenOut != 1 {
		t.Errorf("stolen out = %d, want 1 (P0 donated one task)", rep.Totals.StolenOut)
	}
	if rep.MaxQueueDepth != 5 {
		t.Errorf("max queue depth = %d, want 5", rep.MaxQueueDepth)
	}
	if want := 8.0 / 3.0; math.Abs(rep.MeanQueueDepth-want) > 1e-9 {
		t.Errorf("mean queue depth = %v, want %v", rep.MeanQueueDepth, want)
	}
	if rep.Totals.Blocked != 1 || rep.Totals.Unblocked != 1 || rep.Totals.BlockNanos != 1_500_000 {
		t.Errorf("block accounting = %d/%d/%d, want 1/1/1500000",
			rep.Totals.Blocked, rep.Totals.Unblocked, rep.Totals.BlockNanos)
	}

	var buf bytes.Buffer
	if err := rep.WriteText(&buf); err != nil {
		t.Fatalf("WriteText: %v", err)
	}
	text := buf.String()
	for _, want := range []string{
		"gosched trace report",
		"33.33% of attempts",
		"per-worker",
		"donated",
		"P0",
		"P1",
		"helpers",
		"external",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("report text missing %q\n---\n%s", want, text)
		}
	}
	if strings.Contains(text, "NaN") || strings.Contains(text, "+Inf") {
		t.Errorf("report contains a non-finite number:\n%s", text)
	}
}

// TestParseChromeRejectsForeignJSON makes sure the importer fails loudly on a
// trace that is not ours instead of silently reporting zeroes.
func TestParseChromeRejectsForeignJSON(t *testing.T) {
	if _, err := ParseChrome(strings.NewReader(`{"traceEvents":[{"name":"x","ph":"X","ts":1,"dur":1,"pid":1,"tid":1}]}`)); err == nil {
		t.Fatal("ParseChrome accepted a trace with no gosched_stats metadata")
	}
	if _, err := ParseChrome(strings.NewReader(`not json`)); err == nil {
		t.Fatal("ParseChrome accepted invalid JSON")
	}
}
