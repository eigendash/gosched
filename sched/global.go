package sched

import (
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/eigendash/gosched/trace"
)

// globalQueue is the scheduler's overflow queue: the place every task lands
// before a worker claims it, and the place a blocking worker hands its local
// queue to.
//
// It is sharded because the single-mutex version is the obvious bottleneck: at
// 12 cores, a fork-join workload submits twice per task, and every one of those
// submissions would serialise on one lock. Submitters take a ticket from a
// round-robin counter (one atomic add, no lock) and then touch only their
// shard, so independent submitters rarely share a cache line.
//
// Shards store words rather than Tasks: the queue never interprets what it
// carries, which is what lets the same queue serve both the plain scheduler and
// the traced one without a branch on the hot path. Only the deque that
// ultimately receives the word knows how to read it.
type globalQueue struct {
	shards []gqShard
	ticket atomic.Uint64
	mask   uint64
}

type gqShard struct {
	_     [24]byte // keep adjacent shards off one cache line
	mu    sync.Mutex
	buf   []unsafe.Pointer
	head  int
	tail  int
	count atomic.Int64
}

func newGlobalQueue(shards int) *globalQueue {
	if shards&(shards-1) != 0 {
		panic("sched: global queue shard count must be a power of two")
	}
	q := &globalQueue{shards: make([]gqShard, shards), mask: uint64(shards - 1)}
	for i := range q.shards {
		q.shards[i].buf = make([]unsafe.Pointer, 256)
	}
	return q
}

func (q *globalQueue) push(w unsafe.Pointer) {
	s := &q.shards[q.ticket.Add(1)&q.mask]
	s.mu.Lock()
	if s.tail == len(s.buf) {
		n := copy(s.buf, s.buf[s.head:s.tail])
		for i := n; i < len(s.buf); i++ {
			// Do not retain tasks the queue has already handed out.
			s.buf[i] = nil
		}
		s.head, s.tail = 0, n
		if s.tail == len(s.buf) {
			s.buf = append(s.buf, make([]unsafe.Pointer, len(s.buf))...)
		}
	}
	s.buf[s.tail] = w
	s.tail++
	s.count.Add(1)
	s.mu.Unlock()
}

// len is an unsynchronised sum of the shard counters. It is a gauge for the
// trace, not a correctness input, so it is allowed to be stale by the time it
// returns.
func (q *globalQueue) len() int64 {
	var n int64
	for i := range q.shards {
		n += q.shards[i].count.Load()
	}
	return n
}

// drainInto moves up to max words into d, which must be the caller's own deque
// (Push is owner-only in a Chase-Lev deque, so a thief would be wrong here).
// Shards are visited round-robin from a rotating start so that two drainers do
// not walk into each other.
//
// The words are copied out under the shard lock and pushed into the deque after
// it is released. That split is the difference between a claim that holds a
// shard lock for one memcpy and a claim that holds it for hundreds of atomic
// deque pushes: with the lock held across the pushes, submitters queue behind a
// claim for tens of microseconds, which starves the producer and collapses the
// pool's utilisation. (Measured: producers spent 595 ms of a 624 ms run inside
// Submit before this split.)
//
// scratch must be at least max words and must be owned by the caller; it keeps
// the drain allocation-free.
//
// When traced, an item that has never been claimed in a local queue has its
// holder set to the drainer, which is what lets the trace distinguish "ran
// where it was claimed" from "ran somewhere else" without a per-task lookup.
func (q *globalQueue) drainInto(d *Deque, max int, traced bool, owner int32, scratch []unsafe.Pointer) int {
	if max > len(scratch) {
		max = len(scratch)
	}
	moved := 0
	start := q.ticket.Add(1)
	for i := 0; i < len(q.shards) && moved < max; i++ {
		s := &q.shards[(start+uint64(i))&q.mask]
		if s.count.Load() == 0 {
			continue
		}
		want := max - moved
		s.mu.Lock()
		n := 0
		for n < want && s.head < s.tail {
			scratch[n] = s.buf[s.head]
			s.buf[s.head] = nil
			s.head++
			n++
		}
		if n > 0 {
			// One counter update for the batch instead of one per item.
			s.count.Add(-int64(n))
		}
		if s.head == s.tail {
			s.head, s.tail = 0, 0
		}
		s.mu.Unlock()

		for j := 0; j < n; j++ {
			w := scratch[j]
			if traced {
				if it := (*workItem)(w); it.holder == trace.PExternal {
					it.holder = owner
				}
			}
			d.pushWord(w)
		}
		moved += n
	}
	return moved
}
