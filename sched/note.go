package sched

import (
	"runtime"
	"sync/atomic"
)

// note is a one-shot wakeup register: at most one token is outstanding, and a
// token that arrives while nobody is asleep is remembered rather than lost.
//
// It is a notification, not a count. Two wakes that arrive before the sleeper
// consumes the first collapse into one, and the sleeper sees a single wakeup.
// That is sound for a work-stealing worker because every wake is followed by a
// re-check of every queue, so one wakeup covers all the work that any number of
// submitters queued before it. Callers that need one wakeup per wake must not
// use this type.
//
// It exists because the naive "check a condition, then block on a channel"
// handshake has a lost-wakeup window, and the fix used by runtime schedulers
// (publish "I am about to sleep", re-check, then block) needs a primitive that
// cannot drop a wakeup that raced the re-check. A buffered channel alone cannot
// do this: a second wakeup while the buffer is full would be discarded, and the
// sleeper could then block forever.
//
// The state machine is deliberately tiny:
//
//	idle     - nobody asleep, no token
//	sleeping - the owning worker has parked (or is about to)
//	pending  - a token is waiting to be consumed
//
// sleep is only ever called by the owning worker, so "sleeping" has a single
// writer and the CAS loops below cannot livelock against another sleeper.
type note struct {
	state atomic.Uint32
	ch    chan struct{}
}

const (
	noteIdle uint32 = iota
	noteSleeping
	notePending
)

func newNote() *note {
	return &note{ch: make(chan struct{}, 1)}
}

// wake leaves a token for the worker. It never blocks and never drops a token:
// if the worker is running, the token is recorded in the state so that the next
// sleep returns immediately.
func (n *note) wake() {
	for {
		switch n.state.Load() {
		case noteIdle:
			if n.state.CompareAndSwap(noteIdle, notePending) {
				return
			}
		case noteSleeping:
			if n.state.CompareAndSwap(noteSleeping, notePending) {
				// The channel is buffered, so this cannot block even if the
				// worker is between its state check and its receive. A full
				// buffer means a token is already there, which is just as good.
				select {
				case n.ch <- struct{}{}:
				default:
				}
				return
			}
		default: // notePending: a token is already outstanding.
			return
		}
	}
}

// sleep blocks until a token is available, consuming it.
func (n *note) sleep() {
	for {
		switch n.state.Load() {
		case notePending:
			if n.state.CompareAndSwap(notePending, noteIdle) {
				return
			}
		case noteIdle:
			if !n.state.CompareAndSwap(noteIdle, noteSleeping) {
				continue
			}
			for n.state.Load() == noteSleeping {
				<-n.ch
			}
			// Drain a token that the wake that released us may have pushed, so
			// that the next sleep does not observe a stale one.
			select {
			case <-n.ch:
			default:
			}
			n.state.Store(noteIdle)
			return
		default:
			// Only reachable if two goroutines sleep on the same note, which
			// the worker loop never does. Yield rather than spin hot.
			runtime.Gosched()
		}
	}
}
