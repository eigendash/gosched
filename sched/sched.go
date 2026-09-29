package sched

import (
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/eigendash/gosched/trace"
)

// Errors returned by Submit.
var (
	// ErrClosed is returned once Close has been called.
	ErrClosed = errors.New("sched: scheduler is closed")
	// ErrNilTask rejects a nil task: the deques use a nil slot to mean "never
	// written", so a nil task would be invisible to a thief.
	ErrNilTask = errors.New("sched: nil task")
)

const (
	// maxStealProbes bounds how many victims one idle worker probes before it
	// parks. Probing all P-1 victims is wasted work once the pool is drained;
	// four is the same order as the runtime's own steal budget.
	maxStealProbes = 4
	// maxDrainBatch bounds how much of the shared queue one worker claims at a
	// time. Claiming is what fills the local deque, and a deep local deque is
	// what gives stealing something to do; the bound just keeps one claim from
	// holding a shard lock for an unbounded time. See takeFromGlobal.
	maxDrainBatch = 256
	// helperPollInterval is how often a transient helper checks for work. A
	// helper exists only while every P is blocked, so a short poll is cheaper
	// than building a full park/unpark path for a worker that lives for
	// microseconds.
	helperPollInterval = 50 * time.Microsecond
)

// workItem is what the traced scheduler actually queues. The plain scheduler
// queues bare Tasks; carrying identity costs an allocation, so it is only paid
// when there is a trace to put it in.
type workItem struct {
	task     Task
	id       uint64
	submitNs int64
	// holder is the P that most recently owned this item in a local queue, or
	// trace.PExternal while it has never been claimed. It is what makes the
	// migration count exact: a task executed by a P other than its holder
	// crossed a worker boundary, whether by stealing or by a blocking handoff.
	holder int32
}

// Worker states. The values matter only in that Submit distinguishes "parked"
// from "running" when it decides whom to wake.
const (
	stateRunning int32 = iota
	stateParking
)

type worker struct {
	s      *Scheduler
	dq     *Deque
	note   *note
	id     int32
	helper bool

	state  atomic.Int32
	parked atomic.Bool
	goid   atomic.Uint64

	// rng seeds the victim rotation. It is per worker so that two thieves do
	// not walk through the pool in lockstep, which would make their probes
	// collide on the same victim every round.
	rng uint64

	// scratch is the reusable item used on the untraced path, where the deque
	// stores bare tasks and there is nothing to carry identity in.
	scratch workItem
	// drainBuf stages a claimed batch outside the shard lock, so a claim holds
	// that lock for one memcpy rather than for hundreds of deque pushes.
	drainBuf []unsafe.Pointer
}

// Scheduler is a work-stealing pool of P workers over a shared overflow queue.
type Scheduler struct {
	procs []*worker
	gq    *globalQueue
	tr    *trace.Tracer
	// traced fixes the deque payload for the scheduler's whole lifetime:
	// switching it later would make a thief read a task where an item lives.
	traced bool

	pending atomic.Int64
	closed  atomic.Bool

	// Wait support: a waiter registers a channel, and the task that drops the
	// pending count to zero closes every registered channel while holding wmu.
	wmu     sync.Mutex
	waiters map[chan struct{}]struct{}

	// Blocking support.
	blocked    atomic.Int64
	helpers    atomic.Int32
	helperIDs  chan int32
	helperWG   sync.WaitGroup
	maxHelpers int
	// hmu serialises "may I start a helper?" against "may this helper stop?".
	// Without it there is a window in which a helper has decided to retire but
	// has not yet returned its slot, and a worker that has just blocked finds
	// no slot free and no helper running: the pool then stalls until the
	// blocker's own deadline expires.
	hmu   sync.Mutex
	goids sync.Map // goroutine id -> *worker, consulted only by Blocking

	wg        sync.WaitGroup
	wakeTick  atomic.Uint64
	startOnce sync.Once
}

type config struct {
	procs         int
	traced        bool
	traceCapacity int
	seed          int64
	maxHelpers    int
}

// Option configures a Scheduler.
type Option func(*config)

// WithProcs sets the number of logical processors (worker goroutines). The
// default is runtime.GOMAXPROCS(0).
func WithProcs(n int) Option { return func(c *config) { c.procs = n } }

// WithTracing selects whether scheduling decisions are recorded. Disabling it
// removes both the event stream and the per-task identity allocation, so the
// difference between the two modes is the total price of observability.
func WithTracing(on bool) Option { return func(c *config) { c.traced = on } }

// WithTraceCapacity sets how many events each worker retains before it starts
// dropping them (and counting the drops).
func WithTraceCapacity(n int) Option { return func(c *config) { c.traceCapacity = n } }

// WithSeed fixes the victim-selection randomness, which is what makes a
// single-P trace reproducible and a multi-P trace reproducible in structure.
func WithSeed(s int64) Option { return func(c *config) { c.seed = s } }

// WithMaxHelpers bounds how many transient workers may exist at once when
// every P is blocked. It also bounds the tracer's helper ring count.
func WithMaxHelpers(n int) Option { return func(c *config) { c.maxHelpers = n } }

// New starts a scheduler. Workers are running before New returns.
func New(opts ...Option) *Scheduler {
	cfg := config{
		procs:         runtime.GOMAXPROCS(0),
		traced:        true,
		traceCapacity: trace.DefaultCapacity,
		maxHelpers:    -1, // resolved below
	}
	for _, o := range opts {
		o(&cfg)
	}
	if cfg.procs < 1 {
		cfg.procs = 1
	}
	if cfg.maxHelpers < 0 {
		// One helper per P by default: enough to keep every P worth of work
		// moving when the whole pool is blocked, which is the only situation
		// helpers exist for.
		cfg.maxHelpers = cfg.procs
		if cfg.maxHelpers < 4 {
			cfg.maxHelpers = 4
		}
	}
	if cfg.seed == 0 {
		cfg.seed = time.Now().UnixNano() | 1
	}

	s := &Scheduler{
		gq:         newGlobalQueue(shardCount(cfg.procs)),
		traced:     cfg.traced,
		waiters:    make(map[chan struct{}]struct{}),
		maxHelpers: cfg.maxHelpers,
		helperIDs:  make(chan int32, cfg.maxHelpers),
	}
	for i := 0; i < cfg.maxHelpers; i++ {
		s.helperIDs <- int32(cfg.procs + i)
	}
	// The tracer is sized for the real Ps plus every helper slot plus one ring
	// for work that arrives from outside the pool.
	s.tr = trace.New(cfg.procs, cfg.maxHelpers, cfg.traceCapacity)

	s.procs = make([]*worker, cfg.procs)
	for i := range s.procs {
		s.procs[i] = &worker{
			s:        s,
			dq:       NewDeque(),
			note:     newNote(),
			id:       int32(i),
			rng:      uint64(cfg.seed) + uint64(i)*0x9E3779B97F4A7C15,
			drainBuf: make([]unsafe.Pointer, maxDrainBatch),
		}
	}
	return s
}

// start launches the worker goroutines the first time work actually exists.
//
// Starting them in New would let a worker scan, find nothing and park before
// the caller has submitted anything, which turns the opening park/unpark pair
// into a race. Deferring the start to just after the first task is queued makes
// a single-P trace reproducible, and it means a scheduler that is never used
// never spawns P goroutines.
func (s *Scheduler) start() {
	s.startOnce.Do(func() {
		for _, w := range s.procs {
			s.wg.Add(1)
			go func() {
				defer s.wg.Done()
				w.register()
				defer s.goids.Delete(w.goid.Load())
				w.run()
			}()
		}
	})
}

func shardCount(procs int) int {
	n := 4
	for n < procs*2 && n < 32 {
		n *= 2
	}
	return n
}

func (w *worker) register() {
	id := goid()
	w.goid.Store(id)
	if id != 0 {
		w.s.goids.Store(id, w)
	}
}

// Submit hands a task to the scheduler. Tasks submitted from inside another
// task are the normal way to build a fork-join tree.
//
// Submit must not race Close: a task queued after Close has begun is not
// guaranteed to run. Use Wait before Close to drain the pool.
func (s *Scheduler) Submit(t Task) error {
	if t == nil {
		return ErrNilTask
	}
	if s.closed.Load() {
		return ErrClosed
	}
	var word unsafe.Pointer
	if s.traced {
		id := s.tr.NextTask()
		it := &workItem{task: t, id: id, submitNs: s.tr.Now(), holder: trace.PExternal}
		word = unsafe.Pointer(it)
		s.tr.Submit(trace.PExternal, id, trace.PExternal, s.gq.len())
	} else {
		word = taskToWord(t)
	}
	// The count is incremented before the task is visible so that Wait can
	// never observe zero work while a task is already in flight.
	s.pending.Add(1)
	s.gq.push(word)
	// Start after the push: the workers' first scan must find this task rather
	// than park before it exists.
	s.start()
	s.wakeWorkers()
	if s.blocked.Load() > 0 {
		// Every P may be stuck inside Blocking: without a helper the work we
		// just queued would have nobody to run it, and the blocked task would
		// wait forever for it.
		s.ensureHelper()
	}
	return nil
}

// Wait blocks until the transitive closure of every submitted task has run:
// tasks that submit children while they execute keep the count above zero until
// those children finish too.
func (s *Scheduler) Wait() {
	for {
		if s.pending.Load() == 0 {
			return
		}
		ch := make(chan struct{})
		s.wmu.Lock()
		s.waiters[ch] = struct{}{}
		// Re-check under the lock: the task that dropped the count to zero may
		// have finished between the first check and the registration, and it
		// would then have found no waiter to wake.
		stillPending := s.pending.Load() != 0
		if !stillPending {
			delete(s.waiters, ch)
		}
		s.wmu.Unlock()
		if !stillPending {
			return
		}
		<-ch
	}
}

// Close stops accepting new work, lets the workers drain what is already
// queued, and returns once every worker has exited. It is idempotent.
func (s *Scheduler) Close() error {
	s.closed.Store(true)
	// Start the workers even if no task was ever submitted (they will observe
	// closed and exit immediately), so that wg is always balanced.
	s.start()
	// Wake unconditionally: a worker that is between "no work found" and
	// "published as parked" must not be missed, and a spurious token only costs
	// one extra loop iteration.
	for _, w := range s.procs {
		w.note.wake()
	}
	s.wg.Wait()
	s.helperWG.Wait()
	return nil
}

// Blocking runs fn while telling the scheduler that the calling worker is about
// to wait on something the scheduler cannot see (a syscall, another task, a
// channel owned by the caller).
//
// Three things happen, in this order: the worker's local queue is handed to the
// shared queue so other workers can take it, parked workers are woken so that
// handoff is actually picked up, and — if every P is now blocked while work
// remains — a transient helper worker is started so that the pool cannot
// deadlock waiting for work that only a blocked worker could run.
func (s *Scheduler) Blocking(fn func()) {
	w := s.workerForCaller()
	var start int64
	if w != nil {
		if s.traced {
			start = s.tr.Now()
		}
		handed := w.handoff()
		if s.traced {
			s.tr.Block(w.id, int64(handed), s.pending.Load())
		}
	}
	s.blocked.Add(1)
	s.wakeWorkers()
	s.ensureHelper()

	func() {
		defer s.blocked.Add(-1)
		fn()
	}()

	if w != nil {
		if s.traced {
			s.tr.Unblock(w.id, s.tr.Now()-start)
		}
		// The worker is runnable again; anything it handed off is either done
		// or still queued, and its own deque is empty by construction.
		s.wakeWorkers()
	}
}

// workerForCaller finds the worker whose goroutine is running the current task.
//
// This is the one place the scheduler pays for a goroutine identity: Go has no
// goroutine-local storage, so the only portable way to find the caller's worker
// is to read the goroutine id out of runtime.Stack. That costs hundreds of
// nanoseconds, which is far too much for Submit — so Submit never asks, and
// only this cold path does.
func (s *Scheduler) workerForCaller() *worker {
	id := goid()
	if id == 0 {
		return nil
	}
	if v, ok := s.goids.Load(id); ok {
		return v.(*worker)
	}
	return nil
}

// handoff publishes the caller's local queue to the shared queue. The owner is
// still the only writer of its deque, so this races safely with thieves.
func (w *worker) handoff() int {
	moved := 0
	for {
		word, ok := w.dq.popWord()
		if !ok {
			return moved
		}
		w.s.gq.push(word)
		moved++
	}
}

// ensureHelper starts a transient worker when every executor is currently
// blocked inside Blocking.
//
// The rule is "blocked executors >= executors", not "blocked >= P": a single
// blocked worker in a pool of twelve does not threaten progress, and starting a
// helper for it would spawn a goroutine per blocking call. The spawn and retire
// conditions are deliberately not adjacent (spawn at blocked >= E, retire at
// blocked < E-1) so that a helper does not oscillate.
func (s *Scheduler) ensureHelper() {
	if s.maxHelpers == 0 {
		return
	}
	if s.blocked.Load() < int64(len(s.procs))+int64(s.helpers.Load()) {
		return
	}
	if s.pending.Load() <= 0 {
		return
	}
	s.hmu.Lock()
	defer s.hmu.Unlock()
	if s.blocked.Load() < int64(len(s.procs))+int64(s.helpers.Load()) {
		return
	}
	select {
	case id := <-s.helperIDs:
		s.helpers.Add(1)
		s.helperWG.Add(1)
		go s.runHelper(id)
	default:
		// The helper cap is reached. Progress depends on one of the blocked
		// executors returning, which is why Blocking is cooperative.
	}
}

// runHelper is a transient worker for the all-Ps-blocked case. It has no park
// path: it polls, and it exits as soon as enough real workers are unblocked.
func (s *Scheduler) runHelper(id int32) {
	w := &worker{
		s:        s,
		dq:       NewDeque(),
		id:       id,
		helper:   true,
		rng:      uint64(id)*0x9E3779B97F4A7C15 + 1,
		drainBuf: make([]unsafe.Pointer, maxDrainBatch),
	}
	w.register()
	defer func() {
		s.goids.Delete(w.goid.Load())
		s.helperWG.Done()
	}()
	for {
		if it, ok := w.findWork(maxStealProbes); ok {
			w.execute(it)
			continue
		}
		// Retirement is decided under hmu so that a worker blocking at this
		// instant either finds this helper still on duty or finds the slot free;
		// the slot is returned while the lock is still held, which is what
		// closes the gap.
		s.hmu.Lock()
		executors := int64(len(s.procs)) + int64(s.helpers.Load())
		if s.blocked.Load() < executors-1 || (s.closed.Load() && s.pending.Load() == 0) {
			s.helpers.Add(-1)
			s.helperIDs <- id
			s.hmu.Unlock()
			return
		}
		s.hmu.Unlock()
		time.Sleep(helperPollInterval)
	}
}

func (s *Scheduler) wakeWorkers() {
	n := len(s.procs)
	start := int(s.wakeTick.Add(1)-1) % n
	for i := 0; i < n; i++ {
		w := s.procs[(start+i)%n]
		if w.state.Load() == stateParking {
			w.note.wake()
			// One wake per submission is enough: the woken worker drains a
			// batch and the next submission wakes the next parked worker.
			return
		}
	}
}

// finishTask drops the pending count and releases Wait when it reaches zero.
func (s *Scheduler) finishTask() {
	if s.pending.Add(-1) == 0 {
		s.wmu.Lock()
		for ch := range s.waiters {
			close(ch)
			delete(s.waiters, ch)
		}
		s.wmu.Unlock()
	}
}

func (w *worker) run() {
	s := w.s
	for {
		w.state.Store(stateRunning)
		if it, ok := w.findWork(maxStealProbes); ok {
			w.execute(it)
			continue
		}
		if s.closed.Load() {
			return
		}
		// Publish "parking" *before* the last look for work. A submitter that
		// reads a non-parking state is guaranteed to have queued its task
		// before that read, and therefore before this check.
		w.state.Store(stateParking)
		// The pre-park look sweeps every peer rather than the cheap probe set:
		// a worker that goes to sleep while a peer is holding a batch is a
		// latency bug, and this is the last chance to notice.
		// The item must be executed, not merely observed: findWork removes it
		// from the deque, so discarding the result would lose the task.
		if it, ok := w.findWork(len(s.procs) - 1); ok {
			w.state.Store(stateRunning)
			w.execute(it)
			continue
		}
		if s.closed.Load() {
			return
		}
		w.parked.Store(true)
		if s.traced {
			s.tr.Park(w.id, w.dq.Len(), 0)
		}
		w.note.sleep()
		w.parked.Store(false)
		if s.traced {
			s.tr.Unpark(w.id)
		}
	}
}

func (w *worker) execute(it *workItem) {
	s := w.s
	if s.traced {
		start := s.tr.Now()
		it.task()
		dur := s.tr.Now() - start
		if it.holder != w.id {
			s.tr.Migrate(w.id, it.id, it.holder)
		}
		wait := start - it.submitNs
		if wait < 0 {
			wait = 0
		}
		s.tr.Execute(w.id, it.id, start, dur, wait)
		s.tr.ObserveQueueDepth(w.id, w.dq.Len())
	} else {
		it.task()
	}
	s.finishTask()
}

// popLocal takes the newest item from the worker's own deque.
func (w *worker) popLocal() (*workItem, bool) {
	if w.s.traced {
		return w.dq.PopItem()
	}
	if t, ok := w.dq.Pop(); ok {
		w.scratch = workItem{task: t}
		return &w.scratch, true
	}
	return nil, false
}

// findWork returns the next item to run, claiming from the shared queue before
// it resorts to stealing. probes bounds how many peers are investigated.
func (w *worker) findWork(probes int) (*workItem, bool) {
	if it, ok := w.popLocal(); ok {
		return it, true
	}
	// Claim a slice of the shared queue. Claiming into the local deque is what
	// restores depth-first behaviour and keeps the shared lock off the per-task
	// path.
	if w.takeFromGlobal() > 0 {
		if it, ok := w.popLocal(); ok {
			return it, true
		}
	}
	return w.steal(probes)
}

func (w *worker) takeFromGlobal() int {
	total := w.s.gq.len()
	if total == 0 {
		return 0
	}
	// Half, capped. Halving is deliberately greedy: it is what produces deep
	// local deques and therefore a workload where an idle worker has something
	// worth stealing. A one-at-a-time claim would turn this scheduler into a
	// plain shared-queue pool in which stealing never fires because nobody ever
	// holds work. The cap only bounds how long one shard lock is held.
	take := total / 2
	if take < 1 {
		take = 1
	}
	if take > maxDrainBatch {
		take = maxDrainBatch
	}
	return w.s.gq.drainInto(w.dq, int(take), w.s.traced, w.id, w.drainBuf)
}

func (w *worker) steal(probes int) (*workItem, bool) {
	s := w.s
	n := len(s.procs)
	if n < 2 {
		return nil, false
	}
	if probes > n-1 {
		probes = n - 1
	}
	if probes < 0 {
		probes = 0
	}
	w.rng++
	for i := 0; i < probes; i++ {
		victim := s.procs[(w.rng+uint64(i))%uint64(n)]
		if victim == w {
			continue
		}
		depth := victim.dq.Len()
		if s.traced {
			it, ok := victim.dq.StealItem()
			if !ok {
				s.tr.StealFail(w.id, victim.id, depth)
				continue
			}
			s.tr.Steal(w.id, it.id, victim.id, depth)
			return it, true
		}
		t, ok := victim.dq.Steal()
		if !ok {
			continue
		}
		w.scratch = workItem{task: t}
		return &w.scratch, true
	}
	return nil, false
}

// Stats is a cheap snapshot of pool state, used by tests and diagnostics.
type Stats struct {
	Procs   int
	Pending int64
	Parked  int32
	Blocked int64
	Helpers int32
}

// Stats reports the current pool state. Parked counts workers that have
// published themselves as asleep; it is a diagnostic signal, not a barrier.
func (s *Scheduler) Stats() Stats {
	st := Stats{
		Procs:   len(s.procs),
		Pending: s.pending.Load(),
		Blocked: s.blocked.Load(),
		Helpers: s.helpers.Load(),
	}
	for _, w := range s.procs {
		if w.parked.Load() {
			st.Parked++
		}
	}
	return st
}

// Tracer exposes the tracer so callers can snapshot it or flip recording.
// Flipping Enabled after the scheduler has submitted work is not supported:
// the deque payload is fixed at construction.
func (s *Scheduler) Tracer() *trace.Tracer { return s.tr }

// Trace snapshots the recorded events. Call it once the scheduler is quiescent
// (after Wait, or after Close) so that the snapshot is complete rather than
// merely consistent.
func (s *Scheduler) Trace() *trace.Recording { return s.tr.Snapshot() }
