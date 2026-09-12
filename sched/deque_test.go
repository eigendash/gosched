package sched

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
)

// modelDeque is the reference implementation the real deque is judged against.
// It is deliberately the dumbest thing that can be correct: a slice where index
// 0 is the thief end and the last element is the owner end.
type modelDeque struct {
	items []int
}

func (m *modelDeque) push(id int) { m.items = append(m.items, id) }

func (m *modelDeque) pop() (int, bool) {
	if len(m.items) == 0 {
		return 0, false
	}
	id := m.items[len(m.items)-1]
	m.items = m.items[:len(m.items)-1]
	return id, true
}

func (m *modelDeque) steal() (int, bool) {
	if len(m.items) == 0 {
		return 0, false
	}
	id := m.items[0]
	m.items = m.items[1:]
	return id, true
}

func (m *modelDeque) len() int { return len(m.items) }

// idCollector recovers the identity of a Task. Deque stores bare func() values,
// so identity has to travel inside the closure; the collector is the cheapest
// way to read it back without wrapping tasks or changing the public API.
type idCollector struct{ ids []int }

func (c *idCollector) task(id int) Task {
	return func() { c.ids = append(c.ids, id) }
}

func (c *idCollector) drain() []int {
	out := c.ids
	c.ids = nil
	return out
}

// TestDequeMatchesReferenceModel drives a random mix of push/pop/steal through
// both the real deque and the slice model and requires identical answers,
// including the exact moments the deque reports itself empty. A single-threaded
// run is the only regime where "empty" is unambiguous, which is precisely why
// it is the strongest falsifier for the LIFO/FIFO end semantics.
func TestDequeMatchesReferenceModel(t *testing.T) {
	rng := rand.New(rand.NewSource(0xC0FFEE))
	d := NewDeque()
	model := &modelDeque{}
	col := &idCollector{}
	next := 0

	verifyTask := func(got Task, wantID int) {
		t.Helper()
		got()
		ids := col.drain()
		if len(ids) != 1 || ids[0] != wantID {
			t.Fatalf("task identity: got %v, want [%d]", ids, wantID)
		}
	}

	for step := 0; step < 100000; step++ {
		switch op := rng.Intn(100); {
		case op < 50:
			model.push(next)
			d.Push(col.task(next))
			next++
		case op < 80:
			task, ok := d.Pop()
			wantID, wantOK := model.pop()
			if ok != wantOK {
				t.Fatalf("step %d: Pop ok=%v, want %v", step, ok, wantOK)
			}
			if ok {
				verifyTask(task, wantID)
			}
		default:
			task, ok := d.Steal()
			wantID, wantOK := model.steal()
			if ok != wantOK {
				t.Fatalf("step %d: Steal ok=%v, want %v", step, ok, wantOK)
			}
			if ok {
				verifyTask(task, wantID)
			}
		}
		if got, want := d.Len(), int64(model.len()); got != want {
			t.Fatalf("step %d: Len=%d, want %d", step, got, want)
		}
	}

	// Drain both to prove they still agree at the end, and that growth left no
	// task stranded.
	for {
		task, ok := d.Pop()
		wantID, wantOK := model.pop()
		if ok != wantOK {
			t.Fatalf("final drain: Pop ok=%v, want %v", ok, wantOK)
		}
		if !ok {
			break
		}
		verifyTask(task, wantID)
	}
	if model.len() != 0 {
		t.Fatalf("model still holds %d items", model.len())
	}
}

// TestDequeEndsAreLIFOAndFIFO pins the end semantics the model test relies on:
// owners pop newest-first, thieves take oldest-first.
func TestDequeEndsAreLIFOAndFIFO(t *testing.T) {
	col := &idCollector{}
	d := NewDeque()
	for i := 0; i < 8; i++ {
		d.Push(col.task(i))
	}
	for _, want := range []int{7, 6, 5} {
		task, ok := d.Pop()
		if !ok {
			t.Fatalf("pop %d: deque unexpectedly empty", want)
		}
		task()
		ids := col.drain()
		if len(ids) != 1 || ids[0] != want {
			t.Fatalf("owner pop gave %v, want [%d] (LIFO)", ids, want)
		}
	}
	for _, want := range []int{0, 1} {
		task, ok := d.Steal()
		if !ok {
			t.Fatalf("steal %d: deque unexpectedly empty", want)
		}
		task()
		ids := col.drain()
		if len(ids) != 1 || ids[0] != want {
			t.Fatalf("thief steal gave %v, want [%d] (FIFO)", ids, want)
		}
	}
}

// drainDeque runs the owner-side equivalent of "everything left is mine" so
// concurrent tests can assert exactly-once delivery over the whole task set.
func drainDeque(d *Deque) int {
	n := 0
	for {
		task, ok := d.Pop()
		if !ok {
			return n
		}
		task()
		n++
	}
}

// stealUntilStop is the thief side of every concurrent test: steal and run
// until the stop channel closes. Tasks left behind when a thief notices the
// stop are recovered by the owner's final drain, so no task can be lost even
// though thieves stop at different times.
func stealUntilStop(d *Deque, stop <-chan struct{}, stolen *int64) {
	for {
		task, ok := d.Steal()
		if ok {
			task()
			atomic.AddInt64(stolen, 1)
			continue
		}
		select {
		case <-stop:
			return
		default:
		}
	}
}

// TestDequeGrowthUnderContention forces the double-on-full path while thieves
// are actively draining. Growth is where a Chase-Lev implementation is easiest
// to get wrong: the copy must start at a freshly loaded top, and the owner must
// never write into a slot a thief may still be reading from the old array.
func TestDequeGrowthUnderContention(t *testing.T) {
	const items = 300000
	seen := make([]int32, items)
	var stolen int64

	d := NewDeque()
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			stealUntilStop(d, stop, &stolen)
		}()
	}

	popped := 0
	for i := 0; i < items; i++ {
		id := i
		d.Push(func() { atomic.AddInt32(&seen[id], 1) })
		// Pop occasionally so the deque oscillates around the growth boundary
		// instead of simply growing once to the maximum size.
		if i%7 == 0 {
			if task, ok := d.Pop(); ok {
				task()
				popped++
			}
		}
	}
	close(stop)
	wg.Wait()
	popped += drainDeque(d)

	if d.Cap() <= dequeSlots {
		t.Fatalf("deque never grew: cap=%d", d.Cap())
	}
	for id, n := range seen {
		if n != 1 {
			t.Fatalf("task %d executed %d times, want exactly 1", id, n)
		}
	}
	if total := int64(popped) + stolen; total != items {
		t.Fatalf("retrieved %d tasks, want %d", total, items)
	}
}

// TestDequeConcurrentExactlyOnce is the main concurrency falsifier: one owner
// pushing and popping while several thieves steal. Every task must be delivered
// exactly once; the per-task counter is the reference model, and it catches
// both duplication (a lost CAS or a stale top) and loss (a bottom bookkeeping
// bug), which a "no panic, no deadlock" test would not notice.
func TestDequeConcurrentExactlyOnce(t *testing.T) {
	for _, tc := range []struct {
		name     string
		items    int
		thieves  int
		popEvery int
	}{
		{name: "small_contended_last_element", items: 2000, thieves: 8, popEvery: 1},
		{name: "balanced", items: 50000, thieves: 4, popEvery: 3},
		{name: "thief_heavy", items: 50000, thieves: 8, popEvery: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			seen := make([]int32, tc.items)
			var stolen int64
			d := NewDeque()
			stop := make(chan struct{})

			var wg sync.WaitGroup
			for i := 0; i < tc.thieves; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					stealUntilStop(d, stop, &stolen)
				}()
			}

			popped := 0
			for i := 0; i < tc.items; i++ {
				id := i
				d.Push(func() { atomic.AddInt32(&seen[id], 1) })
				if tc.popEvery > 0 && i%tc.popEvery == 0 {
					if task, ok := d.Pop(); ok {
						task()
						popped++
					}
				}
			}
			close(stop)
			wg.Wait()
			popped += drainDeque(d)

			for id, n := range seen {
				if n != 1 {
					t.Fatalf("task %d retrieved %d times, want exactly 1", id, n)
				}
			}
			if total := int64(popped) + stolen; total != int64(tc.items) {
				t.Fatalf("retrieved %d tasks, want %d", total, tc.items)
			}
		})
	}
}

// TestDequeStealOnEmptyAndSharedSteal checks the degenerate paths explicitly:
// stealing from an empty deque must fail, and several thieves racing for a
// single entry must resolve to exactly one winner.
func TestDequeStealOnEmptyAndSharedSteal(t *testing.T) {
	d := NewDeque()
	if _, ok := d.Steal(); ok {
		t.Fatal("Steal on empty deque returned a task")
	}
	if _, ok := d.Pop(); ok {
		t.Fatal("Pop on empty deque returned a task")
	}

	const rounds = 2000
	col := &idCollector{}
	for round := 0; round < rounds; round++ {
		d.Push(col.task(round))
		var wg sync.WaitGroup
		var wins int32
		for i := 0; i < 4; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if task, ok := d.Steal(); ok {
					atomic.AddInt32(&wins, 1)
					task()
				}
			}()
		}
		wg.Wait()
		if wins != 1 {
			t.Fatalf("round %d: %d thieves won the single entry, want 1", round, wins)
		}
		if ids := col.drain(); len(ids) != 1 || ids[0] != round {
			t.Fatalf("round %d: collected %v", round, ids)
		}
	}
}
