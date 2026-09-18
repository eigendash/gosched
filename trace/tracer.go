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
	// HandedOff counts queued tasks a blocking worker published for others.
	HandedOff int64

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

// helperCapacity caps the rings reserved for transient helper workers. Helpers
// only exist while every P is blocked, they handle far less work than a real P,
// and they are churned frequently, so giving each one a full P-sized buffer
// would spend tens of megabytes to record a few hundred events.
const helperCapacity = 1 << 10

// ringBuf is the allocation behind a ring. It is created on first use and
// published atomically, so a scheduler that never records an event (tracing
// disabled, or a helper that never ran) costs nothing, and a snapshot cannot
// race the allocation.
type ringBuf struct {
	evs []Event
	// seq publishes shared-ring slots. A ring with several writers cannot use
	// "the length is the publication" trick, because writer A can claim index 7
	// while writer B is still filling index 6; seq[i] == i+1 is what tells a
	// reader that slot i is complete.
	seq []atomic.Uint64
}

// ring is one worker's event buffer plus its counters.
type ring struct {
	bufp atomic.Pointer[ringBuf]
	capa int
	// shared marks rings that several goroutines append to: the ring that
	// collects submissions from goroutines the scheduler does not own.
	shared bool
	n      atomic.Int64
	cnts   Counters
}

func (r *ring) buf() *ringBuf {
	if b := r.bufp.Load(); b != nil {
		return b
	}
	b := &ringBuf{evs: make([]Event, r.capa)}
	if r.shared {
		b.seq = make([]atomic.Uint64, r.capa)
	}
	if r.bufp.CompareAndSwap(nil, b) {
		return b
	}
	// Another writer installed a buffer first; use theirs so that every writer
	// publishes into the same slots.
	return r.bufp.Load()
}

// record appends ev unless the buffer is full, in which case the event is
// counted as dropped. The exact counters are updated by the caller, which is
// why a full buffer still yields exact totals.
func (r *ring) record(ev Event) {
	b := r.buf()
	if !r.shared {
		i := r.n.Load()
		if i >= int64(len(b.evs)) {
			atomic.AddInt64(&r.cnts.Dropped, 1)
			return
		}
		b.evs[i] = ev
		// Published last: a reader that sees n also sees the slot contents.
		r.n.Store(i + 1)
		return
	}
	// Shared ring: claim a slot first, so no two writers ever touch the same
	// one, then publish it individually.
	i := r.n.Add(1) - 1
	if i >= int64(len(b.evs)) {
		atomic.AddInt64(&r.cnts.Dropped, 1)
		return
	}
	b.evs[i] = ev
	b.seq[i].Store(uint64(i + 1))
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
		capa := capacity
		if i >= procs && i < n-1 {
			capa = helperCapacity
			if capacity < capa {
				capa = capacity
			}
		}
		t.rings[i] = &ring{capa: capa, shared: i == n-1}
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

// Execute records one completed task run.
//
// start is the timestamp taken immediately before the task ran and dur is the
// wall time it held the worker, both measured by the caller. Taking the reading
// here instead would put the event's timestamp at "whenever this goroutine got
// scheduled to record it", which can be later than events that causally
// followed the task, and would also cost a second clock read per task.
func (t *Tracer) Execute(p int32, task uint64, start, dur, wait int64) {
	if !t.enabled.Load() {
		return
	}
	r := t.ringFor(p)
	atomic.AddInt64(&r.cnts.Executed, 1)
	atomic.AddInt64(&r.cnts.ExecutedNanos, dur)
	r.record(Event{Ts: start, Task: task, Aux: dur, Aux2: wait, P: p, Type: Execute})
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

// Block records a cooperative blocking handoff. handed is how many queued tasks
// the blocking worker published to the shared queue.
func (t *Tracer) Block(p int32, handed, pending int64) {
	if !t.enabled.Load() {
		return
	}
	r := t.ringFor(p)
	atomic.AddInt64(&r.cnts.Blocked, 1)
	atomic.AddInt64(&r.cnts.HandedOff, handed)
	r.record(Event{Ts: t.Now(), Aux: handed, Aux2: pending, P: p, Type: Block})
}

// Unblock ends a blocking window. start is when the window opened and dur is
// how long it lasted; the event is stamped at its end.
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
		HandedOff:       atomic.LoadInt64(&c.HandedOff),
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
		b := r.bufp.Load()
		if b == nil {
			continue
		}
		n := r.n.Load()
		if n > int64(len(b.evs)) {
			n = int64(len(b.evs))
		}
		if !r.shared {
			rec.Events = append(rec.Events, b.evs[:n]...)
			continue
		}
		// A shared ring can have a claimed slot that its writer has not
		// published yet; skip it rather than read a half-written event. That is
		// only reachable if the caller snapshots a running scheduler, which the
		// API documents as unsupported.
		for i := int64(0); i < n; i++ {
			if b.seq[i].Load() == uint64(i+1) {
				rec.Events = append(rec.Events, b.evs[i])
			}
		}
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
	return t.rings[0].capa
}
