package trace

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"sort"
	"strconv"
)

// Chrome Trace Event Format export.
//
// Two decisions worth explaining, because they are what make the file usable
// rather than merely valid:
//
//   - Task execution is emitted as a complete ("X") event so the viewer draws a
//     duration per task. Steals, parks and blocks are instant ("i") events with
//     thread scope, so they sit on the timeline of the P that made the decision.
//   - The exported file carries the exact aggregate counters as a metadata
//     event ("gosched_stats"). A trace whose event buffer overflowed therefore
//     still round-trips into the same analysis: the detail is lossy, the totals
//     are not.
//
// Timestamps in the format are microseconds; the scheduler works in
// nanoseconds, so every timestamp is a float with nanosecond resolution.

const (
	tracePid = 1
	// tidBase keeps synthetic threads (helpers, external submitters) clear of
	// the real P ids, which start at 0.
	tidHelperBase   = 1000
	tidExternalBase = 2000
)

// chromeEvent is one entry of the traceEvents array. Pointers and omitempty are
// used so that metadata events stay terse while timeline events always carry
// the fields the format requires.
type chromeEvent struct {
	Name string         `json:"name"`
	Cat  string         `json:"cat,omitempty"`
	Ph   string         `json:"ph"`
	Ts   float64        `json:"ts"`
	Dur  *float64       `json:"dur,omitempty"`
	Pid  int            `json:"pid"`
	Tid  int            `json:"tid"`
	S    string         `json:"s,omitempty"`
	Args map[string]any `json:"args,omitempty"`
}

// ringStats mirrors Counters with the ring index that produced it, so the
// import path can reconstruct per-P numbers exactly.
type ringStats struct {
	Ring int `json:"ring"`
	Counters
}

type statsArgs struct {
	Procs    int         `json:"procs"`
	Capacity int         `json:"capacity"`
	WallNs   int64       `json:"wall_ns"`
	Dropped  int64       `json:"dropped"`
	Counters []ringStats `json:"counters"`
}

type chromeDoc struct {
	TraceEvents []chromeEvent `json:"traceEvents"`
	DisplayTime string        `json:"displayTimeUnit,omitempty"`
}

// threadOf maps a scheduler P id onto a Chrome thread id. External submitters
// and unattributed helper workers get their own synthetic thread so that the
// timeline never attributes a foreign event to a real P.
func threadOf(p int32, procs int) int {
	switch {
	case p == PExternal:
		return tidExternalBase
	case p < 0:
		return tidExternalBase + 1
	case int(p) < procs:
		return int(p)
	default:
		return tidHelperBase + int(p) - procs
	}
}

// PFromThread is the inverse of threadOf, and is what lets an imported trace be
// analysed exactly like a live one.
func PFromThread(tid, procs int) int32 {
	switch {
	case tid == tidExternalBase:
		return PExternal
	case tid == tidExternalBase+1:
		return PUnknown
	case tid >= tidHelperBase:
		return int32(procs + tid - tidHelperBase)
	default:
		return int32(tid)
	}
}

func nsToMicros(ns int64) float64 { return float64(ns) / 1000 }

// WriteChrome serialises rec in Chrome Trace Event Format.
func WriteChrome(w io.Writer, rec *Recording) error {
	bw := bufio.NewWriterSize(w, 1<<16)
	doc := chromeDoc{DisplayTime: "ns"}
	doc.TraceEvents = make([]chromeEvent, 0, len(rec.Events)+rec.Procs+3)

	meta := func(name string, tid int, args map[string]any) chromeEvent {
		return chromeEvent{Name: name, Ph: "M", Pid: tracePid, Tid: tid, Args: args}
	}
	doc.TraceEvents = append(doc.TraceEvents, meta("process_name", 0, map[string]any{"name": "gosched"}))

	helperSlots := len(rec.PerP) - rec.Procs - 1
	if helperSlots < 0 {
		helperSlots = 0
	}
	for p := 0; p < rec.Procs; p++ {
		doc.TraceEvents = append(doc.TraceEvents, meta("thread_name", p, map[string]any{"name": "P" + strconv.Itoa(p)}))
		doc.TraceEvents = append(doc.TraceEvents, meta("thread_sort_index", p, map[string]any{"sort_index": p}))
	}
	for i := 0; i < helperSlots; i++ {
		tid := tidHelperBase + i
		doc.TraceEvents = append(doc.TraceEvents, meta("thread_name", tid, map[string]any{"name": "helper" + strconv.Itoa(i)}))
	}
	doc.TraceEvents = append(doc.TraceEvents, meta("thread_name", tidExternalBase, map[string]any{"name": "external"}))

	stats := statsArgs{Procs: rec.Procs, Capacity: rec.Capacity, WallNs: rec.WallNanos, Dropped: rec.Dropped}
	for i := range rec.PerP {
		stats.Counters = append(stats.Counters, ringStats{Ring: i, Counters: rec.PerP[i]})
	}
	var statsArg any = stats
	doc.TraceEvents = append(doc.TraceEvents, meta("gosched_stats", 0, map[string]any{"gosched_stats": statsArg}))

	for _, ev := range rec.Events {
		tid := threadOf(ev.P, rec.Procs)
		ce := chromeEvent{Name: ev.Type.String(), Cat: "gosched", Pid: tracePid, Tid: tid, Ts: nsToMicros(ev.Ts)}
		switch ev.Type {
		case Execute:
			dur := nsToMicros(ev.Aux)
			ce.Name = "task"
			ce.Ph = "X"
			ce.Dur = &dur
			ce.Args = map[string]any{"task": ev.Task, "depth": ev.Aux2}
		case Submit:
			ce.Ph = "i"
			ce.S = "t"
			ce.Args = map[string]any{"task": ev.Task, "depth": ev.Aux, "origin": ev.Aux2}
		case Steal:
			ce.Ph = "i"
			ce.S = "t"
			ce.Args = map[string]any{"task": ev.Task, "victim": ev.Aux, "victim_depth": ev.Aux2}
		case StealFail:
			ce.Ph = "i"
			ce.S = "t"
			ce.Args = map[string]any{"victim": ev.Aux, "victim_depth": ev.Aux2}
		case Migrate:
			ce.Ph = "i"
			ce.S = "t"
			ce.Args = map[string]any{"task": ev.Task, "origin": ev.Aux}
		case Park:
			ce.Ph = "i"
			ce.S = "t"
			ce.Args = map[string]any{"depth": ev.Aux, "idle_rounds": ev.Aux2}
		case Unpark:
			ce.Ph = "i"
			ce.S = "t"
		case Block:
			ce.Ph = "i"
			ce.S = "t"
			ce.Args = map[string]any{"pending": ev.Aux}
		case Unblock:
			ce.Ph = "i"
			ce.S = "t"
			ce.Args = map[string]any{"blocked_ns": ev.Aux}
		default:
			ce.Ph = "i"
			ce.S = "t"
			ce.Args = map[string]any{"aux": ev.Aux, "aux2": ev.Aux2}
		}
		doc.TraceEvents = append(doc.TraceEvents, ce)
	}

	enc := json.NewEncoder(bw)
	if err := enc.Encode(doc); err != nil {
		return fmt.Errorf("encode trace: %w", err)
	}
	return bw.Flush()
}

// ParseChrome reads a trace written by WriteChrome back into a Recording. This
// exists so the report tool can be pointed at a committed file and produce the
// same numbers the live run produced; it is also the strongest possible check
// that the export did not silently drop a field the analysis needs.
func ParseChrome(r io.Reader) (*Recording, error) {
	var doc chromeDoc
	dec := json.NewDecoder(bufio.NewReaderSize(r, 1<<16))
	if err := dec.Decode(&doc); err != nil {
		return nil, fmt.Errorf("decode trace: %w", err)
	}
	rec := &Recording{}
	byType := map[string]EventType{}
	for i, name := range eventNames {
		if name != "" {
			byType[name] = EventType(i)
		}
	}
	byType["task"] = Execute

	// The statistics metadata is read first so that thread ids can be mapped
	// back onto P ids while the events are decoded; the writer always emits it
	// before the timeline, but this pass makes the reader order-independent.
	for _, ce := range doc.TraceEvents {
		if ce.Ph != "M" || ce.Name != "gosched_stats" {
			continue
		}
		raw, ok := ce.Args["gosched_stats"]
		if !ok {
			return nil, fmt.Errorf("gosched_stats metadata missing payload")
		}
		// Re-marshal through JSON because the payload arrived as a generic
		// map; this keeps the decoder honest about field names.
		blob, err := json.Marshal(raw)
		if err != nil {
			return nil, err
		}
		var stats statsArgs
		if err := json.Unmarshal(blob, &stats); err != nil {
			return nil, fmt.Errorf("decode gosched_stats: %w", err)
		}
		rec.Procs = stats.Procs
		rec.Capacity = stats.Capacity
		rec.WallNanos = stats.WallNs
		rec.Dropped = stats.Dropped
		n := stats.Procs
		for _, rs := range stats.Counters {
			if rs.Ring+1 > n {
				n = rs.Ring + 1
			}
		}
		rec.PerP = make([]Counters, n)
		for _, rs := range stats.Counters {
			rec.PerP[rs.Ring] = rs.Counters
		}
	}
	if rec.Procs == 0 {
		return nil, fmt.Errorf("trace has no gosched_stats metadata: not a gosched trace")
	}

	for _, ce := range doc.TraceEvents {
		if ce.Ph == "M" {
			continue
		}
		typ, ok := byType[ce.Name]
		if !ok {
			return nil, fmt.Errorf("unknown trace event name %q", ce.Name)
		}
		ev := Event{
			Ts:   roundNanos(ce.Ts),
			Task: uint64Arg(ce.Args, "task"),
			P:    PFromThread(ce.Tid, rec.Procs),
			Type: typ,
		}
		switch typ {
		case Execute:
			// ts already carries the start of the task; dur is the span.
			ev.Aux = int64(math.Round(deref(ce.Dur) * 1000))
			ev.Aux2 = int64Arg(ce.Args, "depth")
		case Submit:
			ev.Aux = int64Arg(ce.Args, "depth")
			ev.Aux2 = int64Arg(ce.Args, "origin")
		case Steal:
			ev.Aux = int64Arg(ce.Args, "victim")
			ev.Aux2 = int64Arg(ce.Args, "victim_depth")
		case StealFail:
			ev.Aux = int64Arg(ce.Args, "victim")
			ev.Aux2 = int64Arg(ce.Args, "victim_depth")
		case Migrate:
			ev.Aux = int64Arg(ce.Args, "origin")
		case Park:
			ev.Aux = int64Arg(ce.Args, "depth")
			ev.Aux2 = int64Arg(ce.Args, "idle_rounds")
		case Block:
			ev.Aux = int64Arg(ce.Args, "pending")
		case Unblock:
			ev.Aux = int64Arg(ce.Args, "blocked_ns")
		}
		rec.Events = append(rec.Events, ev)
	}
	sort.SliceStable(rec.Events, func(a, b int) bool { return rec.Events[a].Ts < rec.Events[b].Ts })
	return rec, nil
}

// roundNanos converts a microsecond timestamp back to nanoseconds, rounding
// rather than truncating so that a round trip does not lose the last digit to
// binary floating point representation.
func roundNanos(micros float64) int64 { return int64(math.Round(micros * 1000)) }

func deref(f *float64) float64 {
	if f == nil {
		return 0
	}
	return *f
}

func int64Arg(args map[string]any, key string) int64 {
	v, ok := args[key]
	if !ok {
		return 0
	}
	f, ok := v.(float64)
	if !ok {
		return 0
	}
	return int64(f)
}

func uint64Arg(args map[string]any, key string) uint64 {
	return uint64(int64Arg(args, key))
}
