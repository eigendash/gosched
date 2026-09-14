// Package trace records scheduler decisions and turns them into something a
// human can read: a Chrome Trace Event file and an aggregate report.
//
// The design constraint that shapes everything here is that tracing must be
// affordable on the scheduler's hot path. Recording therefore goes into a
// per-P, fixed-capacity buffer with no shared lock: a worker appends to its own
// buffer, publishes the new length with one atomic store, and never waits for
// an exporter. When a buffer overflows, events for that P are dropped and
// counted — the *aggregates*, which are maintained as plain atomic counters,
// stay exact.
package trace

import (
	"fmt"
	"strconv"
)

// EventType enumerates the decisions worth recording. The first nine are the
// ones a reviewer needs to explain a run: where work came from, who took it,
// when a worker slept, and how long each task occupied a P.
type EventType uint8

const (
	// Submit is recorded by the submitting goroutine; P is the submitter, or
	// PExternal for a goroutine the scheduler does not own.
	Submit EventType = iota
	// Steal is a successful transfer out of a victim's local deque.
	Steal
	// StealFail is a probe that found the victim empty (or lost the CAS race).
	StealFail
	// Migrate is recorded when a task executes on a P other than the one that
	// submitted it, whether it moved by stealing or through the global queue.
	Migrate
	// Park is a worker publishing itself as asleep with no work available.
	Park
	// Unpark is a worker being woken by a submitter or by Close.
	Unpark
	// Execute covers one task's run on one P, including its duration.
	Execute
	// Block is a cooperative handoff: a task told the scheduler it is about to
	// wait on something the scheduler cannot see.
	Block
	// Unblock ends a Block window.
	Unblock
)

var eventNames = [...]string{
	Submit:    "submit",
	Steal:     "steal",
	StealFail: "steal_fail",
	Migrate:   "migrate",
	Park:      "park",
	Unpark:    "unpark",
	Execute:   "execute",
	Block:     "block",
	Unblock:   "unblock",
}

func (t EventType) String() string {
	if int(t) < len(eventNames) {
		return eventNames[t]
	}
	return "event(" + strconv.Itoa(int(t)) + ")"
}

// PExternal marks work submitted by a goroutine that is not one of the
// scheduler's workers, such as a main function feeding the pool.
const PExternal int32 = -1

// PUnknown marks an event whose originating P cannot be attributed (a helper
// worker that is not tied to a logical processor, for example).
const PUnknown int32 = -2

// Event is a single recorded decision. It is a flat, fixed-size struct: no
// pointers, no slices, so appending one to a per-P buffer neither allocates nor
// gives the garbage collector anything to trace.
type Event struct {
	// Ts is nanoseconds since the tracer was created.
	Ts int64
	// Task is the scheduler-assigned task sequence number, 0 when the event is
	// not about one specific task.
	Task uint64
	// Aux and Aux2 carry type-specific payload:
	//
	//	Submit     Aux=queue depth at submit,      Aux2=origin P
	//	Execute    Aux=duration in ns,             Aux2=local queue depth at start
	//	Steal      Aux=victim P,                   Aux2=victim queue depth
	//	StealFail  Aux=victim P,                   Aux2=victim queue depth
	//	Migrate    Aux=originating P,              Aux2=0
	//	Park       Aux=local queue depth,          Aux2=idle streak
	//	Unpark     Aux=0,                          Aux2=0
	//	Block      Aux=pending tasks at block time, Aux2=0
	//	Unblock    Aux=blocked duration in ns,      Aux2=0
	Aux int64
	// Aux2 is the second payload word; see Aux.
	Aux2 int64
	// P is the logical processor that recorded the event.
	P int32
	// Type is the decision kind.
	Type EventType
}

func (e Event) String() string {
	return fmt.Sprintf("%s(p=%d task=%d aux=%d aux2=%d)@%dns",
		e.Type, e.P, e.Task, e.Aux, e.Aux2, e.Ts)
}
