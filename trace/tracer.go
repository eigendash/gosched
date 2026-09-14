package trace

import (
	"sort"
	"sync/atomic"
	"time"
)

// Counters are the exact per-ring aggregates. They are maintained as atomic
// counters alongside the event buffer rather than derived from it, so a lossy
// buffer degrades detail without distorting the totals: the analysis can still
// say "17,412 steals succeeded" even if the events describing them overflowed.
type Counters struct {
	Submits       int64
	Executed      int64
	ExecutedNanos int64

	StealAttempts  int64
	StealSuccesses int64
	StolenIn       int64 // tasks this worker took from another worker's deque
	StolenOut      int64 // tasks thieves took from this worker's deque
	ProbedEmpty    int64 // failed steal probes absorbed by this worker

	MigrationsIn int64 // tasks executed here that were submitted elsewhere

	Parked     int64
	Unparked   int64
	Blocked    int64
	Unblocked  int64
	BlockNanos int64

	QueueDepthSum   int64
	QueueDepthMax   int64
	QueueDepthCount int64

	Dropped int64
}

// DefaultCapacity is the number of events one worker can retain. Overflow drops
// the newest events and counts them, which keeps the buffer single-writer and
// lock-free at the cost of losing the tail of a very busy trace. Each slot
// costs sizeof(Event) = 40 bytes.
const DefaultCapacity = 1 << 16

// ring is one worker's event buffer plus its counters. Exactly one goroutine
// records into a given ring, so appends need no lock; a reader only looks at
// slots below the published length, which is what makes a quiescent snapshot
// safe without a barrier on the hot path.
type ring struct {
	buf  []Event
	n    atomic.Int64
	cnts Counters
}

func (r *ring) record(ev Event) {
	i := r.n.Load()
	if i >= int64(len(r.buf)) {
		atomic.AddInt64(&r.cnts.Dropped, 1)
		return
	}
	r.buf[i] = ev
	// Published last: a reader that sees n also sees the slot contents.
	r.n.Store(i + 1)
}

// Tracer owns the per-worker rings and the scheduler's clock origin.
//
// Rings are laid out as [procs real P][helperSlots transient workers][1 sink
// for work submitted from outside the scheduler]. That way every event lands in
// the bucket it belongs to and no counter is silently shared.
type Tracer struct {
	procs       int
	helperSlots int
	start       time.Time
	rings       []*ring
	enabled     atomic.Bool
	seq         atomic.Uint64
}

// New creates a tracer for procs logical processors plus helperSlots transient
// workers, each with capacity events of detail.
func New(procs, helperSlots, capacity int) *Tracer {
	if procs < 1 {
		procs = 1
	}
	if helperSlots < 0 {
		helperSlots = 0
	}
	if capacity < 1 {
		capacity = 1
	}
	t := &Tracer{
		procs:       procs,
		helperSlots: helperSlots,
		start:       time.Now(),
	}
	n := procs + helperSlots + 1
	t.rings = make([]*ring, n)
	for i := range t.rings {
		t.rings[i] = &ring{buf: make([]Event, capacity)}
	}
	t.enabled.Store(true)
	return t
}

// SetEnabled turns recording on or off. The scheduler checks Enabled before it
// even reads the clock, so the disabled path costs one atomic load per event
// site and nothing else.
func (t *Tracer) SetEnabled(on bool) { t.enabled.Store(on) }

// Enabled reports whether events are currently recorded.
func (t *Tracer) Enabled() bool { return t.enabled.Load() }

// Procs is the number of real logical processors this tracer was sized for.
func (t *Tracer) Procs() int { return t.procs }

// Rings is the number of per-worker buffers, including helpers and the sink.
func (t *Tracer) Rings() int { return len(t.rings) }

// sinkIndex is the ring used by anything that is not one of the scheduler's
// workers: external submitters, and any helper beyond the reserved slots.
func (t *Tracer) sinkIndex() int { return len(t.rings) - 1 }

func (t *Tracer) ringFor(p int32) *ring {
	if p >= 0 && int(p) < t.sinkIndex() {
		return t.rings[p]
	}
	return t.rings[t.sinkIndex()]
}

// NextTask hands out the task ids that make a trace readable.
func (t *Tracer) NextTask() uint64 { return t.seq.Add(1) }

// Now returns nanoseconds since the tracer was created. The scheduler calls it
// once per task rather than once per event site, which keeps instrumentation
// cost proportional to tasks rather than to scheduling churn.
func (t *Tracer) Now() int64 { return time.Since(t.start).Nanoseconds() }

// Submit records a task entering the scheduler.
func (t *Tracer) Submit(p int32, task uint64, origin int32, depth int64) {
	if !t.enabled.Load() {
		return
	}
	r := t.ringFor(p)
	atomic.AddInt64(&r.cnts.Submits, 1)
	r.record(Event{Ts: t.Now(), Task: task, Aux: depth, Aux2: int64(origin), P: p, Type: Submit})
}

// Execute records one completed task run. dur is the wall time the task held
// the worker.
func (t *Tracer) Execute(p int32, task uint64, dur int64, depth int64) {
	if !t.enabled.Load() {
		return
	}
	r := t.ringFor(p)
	atomic.AddInt64(&r.cnts.Executed, 1)
	atomic.AddInt64(&r.cnts.ExecutedNanos, dur)
	r.record(Event{Ts: t.Now() - dur, Task: task, Aux: dur, Aux2: depth, P: p, Type: Execute})
}

// Steal records a successful transfer, crediting the victim's exact counter so
// that "who donated work" survives even if the victim's own buffer overflows.
func (t *Tracer) Steal(p int32, task uint64, victim int32, victimDepth int64) {
	if !t.enabled.Load() {
		return
	}
	r := t.ringFor(p)
	atomic.AddInt64(&r.cnts.StealAttempts, 1)
	atomic.AddInt64(&r.cnts.StealSuccesses, 1)
	atomic.AddInt64(&r.cnts.StolenIn, 1)
	atomic.AddInt64(&t.ringFor(victim).cnts.StolenOut, 1)
	r.record(Event{Ts: t.Now(), Task: task, Aux: int64(victim), Aux2: victimDepth, P: p, Type: Steal})
}

// StealFail records a probe that found nothing.
func (t *Tracer) StealFail(p int32, victim int32, victimDepth int64) {
	if !t.enabled.Load() {
		return
	}
	r := t.ringFor(p)
	atomic.AddInt64(&r.cnts.StealAttempts, 1)
	atomic.AddInt64(&t.ringFor(victim).cnts.ProbedEmpty, 1)
	r.record(Event{Ts: t.Now(), Aux: int64(victim), Aux2: victimDepth, P: p, Type: StealFail})
}

// Migrate records that a task ran somewhere other than where it was submitted.
func (t *Tracer) Migrate(p int32, task uint64, origin int32) {
	if !t.enabled.Load() {
		return
	}
	r := t.ringFor(p)
	atomic.AddInt64(&r.cnts.MigrationsIn, 1)
	r.record(Event{Ts: t.Now(), Task: task, Aux: int64(origin), P: p, Type: Migrate})
}

// Park records a worker going to sleep.
func (t *Tracer) Park(p int32, depth int64, idleRounds int64) {
	if !t.enabled.Load() {
		return
	}
	r := t.ringFor(p)
	atomic.AddInt64(&r.cnts.Parked, 1)
	r.record(Event{Ts: t.Now(), Aux: depth, Aux2: idleRounds, P: p, Type: Park})
}

// Unpark records a worker waking up.
func (t *Tracer) Unpark(p int32) {
	if !t.enabled.Load() {
		return
	}
	r := t.ringFor(p)
	atomic.AddInt64(&r.cnts.Unparked, 1)
	r.record(Event{Ts: t.Now(), P: p, Type: Unpark})
}

// Block records a cooperative blocking handoff.
func (t *Tracer) Block(p int32, pending int64) {
	if !t.enabled.Load() {
		return
	}
	r := t.ringFor(p)
	atomic.AddInt64(&r.cnts.Blocked, 1)
	r.record(Event{Ts: t.Now(), Aux: pending, P: p, Type: Block})
}

// Unblock ends a blocking window and records how long it lasted.
func (t *Tracer) Unblock(p int32, dur int64) {
	if !t.enabled.Load() {
		return
	}
	r := t.ringFor(p)
	atomic.AddInt64(&r.cnts.Unblocked, 1)
	atomic.AddInt64(&r.cnts.BlockNanos, dur)
	r.record(Event{Ts: t.Now(), Aux: dur, P: p, Type: Unblock})
}

// ObserveQueueDepth folds one queue-depth sample into a worker's exact
// statistics. Queue depth is a gauge rather than a decision, so it is kept out
// of the event stream: sampling it per event would dominate the cost of the
// trace it is supposed to explain.
func (t *Tracer) ObserveQueueDepth(p int32, depth int64) {
	if !t.enabled.Load() {
		return
	}
	r := t.ringFor(p)
	atomic.AddInt64(&r.cnts.QueueDepthSum, depth)
	atomic.AddInt64(&r.cnts.QueueDepthCount, 1)
	for {
		old := atomic.LoadInt64(&r.cnts.QueueDepthMax)
		if depth <= old || atomic.CompareAndSwapInt64(&r.cnts.QueueDepthMax, old, depth) {
			return
		}
	}
}

// Recording is a point-in-time copy of everything a tracer observed. Building
// one is only safe when no goroutine is recording any more, which in practice
// means after the scheduler has been closed.
type Recording struct {
	// Procs is the number of real logical processors; PerP entries beyond it
	// belong to transient helper workers or to the external-submitter sink.
	Procs     int
	Capacity  int
	WallNanos int64
	Events    []Event
	PerP      []Counters
	Dropped   int64
}

func loadCounters(c *Counters) Counters {
	return Counters{
		Submits:         atomic.LoadInt64(&c.Submits),
		Executed:        atomic.LoadInt64(&c.Executed),
		ExecutedNanos:   atomic.LoadInt64(&c.ExecutedNanos),
		StealAttempts:   atomic.LoadInt64(&c.StealAttempts),
		StealSuccesses:  atomic.LoadInt64(&c.StealSuccesses),
		StolenIn:        atomic.LoadInt64(&c.StolenIn),
		StolenOut:       atomic.LoadInt64(&c.StolenOut),
		ProbedEmpty:     atomic.LoadInt64(&c.ProbedEmpty),
		MigrationsIn:    atomic.LoadInt64(&c.MigrationsIn),
		Parked:          atomic.LoadInt64(&c.Parked),
		Unparked:        atomic.LoadInt64(&c.Unparked),
		Blocked:         atomic.LoadInt64(&c.Blocked),
		Unblocked:       atomic.LoadInt64(&c.Unblocked),
		BlockNanos:      atomic.LoadInt64(&c.BlockNanos),
		QueueDepthSum:   atomic.LoadInt64(&c.QueueDepthSum),
		QueueDepthMax:   atomic.LoadInt64(&c.QueueDepthMax),
		QueueDepthCount: atomic.LoadInt64(&c.QueueDepthCount),
		Dropped:         atomic.LoadInt64(&c.Dropped),
	}
}

// Snapshot copies the recorded events and the exact counters. Callers must
// ensure the scheduler is quiescent: a snapshot taken while workers record can
// see a shorter event list (a slot is published only after it is written) but
// never a torn one.
func (t *Tracer) Snapshot() *Recording {
	rec := &Recording{
		Procs:     t.procs,
		Capacity:  t.capacityPerRing(),
		WallNanos: t.Now(),
		PerP:      make([]Counters, len(t.rings)),
	}
	for i, r := range t.rings {
		rec.PerP[i] = loadCounters(&r.cnts)
		rec.Dropped += rec.PerP[i].Dropped
		n := r.n.Load()
		if n > int64(len(r.buf)) {
			n = int64(len(r.buf))
		}
		rec.Events = append(rec.Events, r.buf[:n]...)
	}
	// Merge order matters for the determinism test: sort by timestamp and keep
	// per-ring order for ties so two identical runs produce identical slices.
	sort.SliceStable(rec.Events, func(a, b int) bool {
		return rec.Events[a].Ts < rec.Events[b].Ts
	})
	return rec
}

func (t *Tracer) capacityPerRing() int {
	if len(t.rings) == 0 {
		return 0
	}
	return len(t.rings[0].buf)
}
