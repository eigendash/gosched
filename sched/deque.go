// Package sched implements a work-stealing task scheduler whose scheduling
// decisions are recorded as trace events.
package sched

import (
	"sync/atomic"
	"unsafe"
)

// Task is the unit of work. Tasks are plain functions so that the scheduler
// imposes no interface/allocation tax on callers; the price is that a task has
// no way to signal a result other than through a closure it captured.
type Task func()

// dequeSlots is the initial slot count of a deque. It is a power of two because
// indexing uses a mask rather than a modulo, and the mask is what keeps the
// owner's push/pop path branch-light.
const dequeSlots = 64

// dequeBuf is an immutable-in-place slot array: growth allocates a new buffer
// and publishes it, so a thief holding a stale buffer always sees a consistent
// (if outdated) array rather than torn contents.
type dequeBuf struct {
	slots []unsafe.Pointer
}

// Slots are accessed atomically even though a single owner writes them.
//
// A thief reads top before bottom, so its top can be stale by the time it reads
// a slot; if other thieves meanwhile advance top and the owner wraps around the
// buffer, the owner's write and the thief's read can hit the same slot. The
// read value is always discarded in that case (the CAS on top fails), so the
// algorithm is still correct — but in Go terms it is a plain data race and
// therefore undefined behaviour. Accessing slots atomically makes the race
// benign by construction, and is also what makes the deque sound on a weakly
// ordered machine such as arm64. func() values are a single pointer, so this
// costs one release store per push on arm64, not an allocation.

func taskToWord(t Task) unsafe.Pointer {
	return *(*unsafe.Pointer)(unsafe.Pointer(&t))
}

func wordToTask(p unsafe.Pointer) Task {
	return *(*Task)(unsafe.Pointer(&p))
}

func (b *dequeBuf) mask() int64 { return int64(len(b.slots)) - 1 }

func (b *dequeBuf) load(i int64) Task {
	return wordToTask(atomic.LoadPointer(&b.slots[i&b.mask()]))
}

func (b *dequeBuf) store(i int64, t Task) {
	atomic.StorePointer(&b.slots[i&b.mask()], taskToWord(t))
}

func newDequeBuf(slots int) *dequeBuf {
	return &dequeBuf{slots: make([]unsafe.Pointer, slots)}
}

// Deque is a Chase-Lev work-stealing deque.
//
// One goroutine (the owner) calls Push and Pop; any number of other goroutines
// call Steal. The owner end is LIFO, which gives depth-first fork-join
// behaviour and keeps a task's children on the core that is already warm for
// them; the thief end is FIFO, so the coldest task leaves first.
//
// Synchronisation is built from Go's sequentially consistent atomics rather
// than raw fences: since Go 1.19 the sync/atomic operations are documented as
// SC, so the release/acquire pair Chase's algorithm needs is implied by the
// atomic store of bottom and the atomic load of bottom in Steal.
//
// The capacity invariant is what keeps the algorithm correct: the owner grows
// before it would write slot b while b-top >= len(slots), so the entries in
// [top, bottom) always occupy distinct slots in the currently published buffer.
type Deque struct {
	// top is the thief end. Thieves advance it with CAS; the owner only reads it.
	top atomic.Int64
	// bottom is the owner end. Only the owner stores to it.
	bottom atomic.Int64
	buf    atomic.Pointer[dequeBuf]
}

// NewDeque returns an empty deque with the default initial capacity.
func NewDeque() *Deque {
	d := &Deque{}
	d.buf.Store(newDequeBuf(dequeSlots))
	return d
}

// Push appends t at the owner end. Only the owner may call Push. t must not be
// nil: a nil slot is how a never-written entry is recognised.
func (d *Deque) Push(t Task) {
	b := d.bottom.Load()
	buf := d.buf.Load()
	if b-d.top.Load() >= int64(len(buf.slots)) {
		buf = d.grow(b)
	}
	buf.store(b, t)
	d.bottom.Store(b + 1)
}

// grow doubles the slot array. The caller (the owner) has already observed that
// the deque holds at least len(slots) live entries, so doubling guarantees a
// free slot; the copy starts at a freshly loaded top because thieves may have
// drained entries while we decided to grow, and a stale top would make us copy
// slots that were already claimed.
func (d *Deque) grow(b int64) *dequeBuf {
	old := d.buf.Load()
	next := newDequeBuf(len(old.slots) * 2)
	t := d.top.Load()
	for i := t; i < b; i++ {
		next.store(i, old.load(i))
	}
	d.buf.Store(next)
	return next
}

// Pop removes the newest task. Only the owner may call Pop. It reports false
// when the deque is empty, or when a thief won the race for the last entry.
func (d *Deque) Pop() (Task, bool) {
	b := d.bottom.Load() - 1
	d.bottom.Store(b)
	t := d.top.Load()
	if t > b {
		// Empty: undo the speculative decrement so Len stays meaningful.
		d.bottom.Store(b + 1)
		return nil, false
	}
	buf := d.buf.Load()
	task := buf.load(b)
	if t == b {
		// The last entry is contested: a thief that reads top == t may take it.
		// The CAS decides the single winner; the loser restores bottom.
		if !d.top.CompareAndSwap(t, t+1) {
			d.bottom.Store(b + 1)
			return nil, false
		}
		d.bottom.Store(b + 1)
	}
	if task == nil {
		// Defensive: a nil slot with a live index means the caller raced a grow
		// into territory this buffer never held. Undo the decrement so the
		// deque is not left short by one entry.
		if t != b {
			d.bottom.Store(b + 1)
		}
		return nil, false
	}
	return task, true
}

// Steal removes the oldest task. Any goroutine other than the owner may call
// Steal, including concurrently with other thieves.
func (d *Deque) Steal() (Task, bool) {
	t := d.top.Load()
	b := d.bottom.Load()
	if t >= b {
		return nil, false
	}
	buf := d.buf.Load()
	task := buf.load(t)
	if !d.top.CompareAndSwap(t, t+1) {
		// Another thief (or the owner popping the last entry) moved top first;
		// the value this call read may already belong to that winner.
		return nil, false
	}
	if task == nil {
		// A stale t that predates a grow can name a slot this buffer never
		// held; the CAS above can only have succeeded if top had not advanced,
		// so this is a defensive check rather than a reachable steady state.
		return nil, false
	}
	return task, true
}

// Len is an instantaneous, unsynchronised estimate of the number of queued
// tasks. It exists for queue-depth telemetry; it can be stale the moment it
// returns, and concurrent draining can make it transiently overshoot.
func (d *Deque) Len() int64 {
	n := d.bottom.Load() - d.top.Load()
	if n < 0 {
		return 0
	}
	return n
}

// Cap reports the current slot count. Growth is amortised doubling, so a thief
// that observes a small cap knows it is looking at a young deque.
func (d *Deque) Cap() int {
	return len(d.buf.Load().slots)
}
