// Package bench compares three ways of running the same tasks: a naive
// goroutine per task, a fixed pool over one shared channel, and the work
// stealing scheduler.
//
// Everything here is written so that the three implementations run *identical*
// work: the same Shape drives all of them through the same Spawner surface, and
// the only difference is how the work is scheduled. A baseline that ran
// different code would make every number meaningless.
package bench

import (
	"sort"
	"sync"
	"time"
)

// Task is the unit of work every implementation accepts.
type Task func()

// Spawner is the surface a workload is written against. Block is part of it
// because the scheduler can do something useful when a task announces that it
// is about to wait, while the baselines can only run the function.
type Spawner interface {
	Submit(Task)
	Wait()
	Close()
	Block(func())
}

// Spin burns CPU for at least d. It is the unit of "work" in every CPU-bound
// workload, so all three implementations pay exactly the same cost per task.
func Spin(d time.Duration) {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
	}
}

// Latency records submit-to-completion times for the shapes where tail latency
// is the point. Each sample is written by exactly one task at a unique index,
// so there is no lock on the task path and no measurement distortion from the
// benchmark harness itself.
type Latency struct {
	durs []int64
}

// NewLatency prepares a recorder for n samples.
func NewLatency(n int) *Latency { return &Latency{durs: make([]int64, n)} }

// Record stores one sample. idx must be unique per task.
func (l *Latency) Record(idx int, d time.Duration) {
	if l == nil || idx < 0 || idx >= len(l.durs) {
		return
	}
	l.durs[idx] = int64(d)
}

// Percentiles returns p50, p99 and max as durations. It must be called after
// Wait, which is what publishes the samples written by the workers.
func (l *Latency) Percentiles() (p50, p99, max time.Duration) {
	if l == nil || len(l.durs) == 0 {
		return 0, 0, 0
	}
	s := make([]int64, len(l.durs))
	copy(s, l.durs)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	pick := func(p int) time.Duration {
		i := len(s) * p / 100
		if i >= len(s) {
			i = len(s) - 1
		}
		return time.Duration(s[i])
	}
	return pick(50), pick(99), time.Duration(s[len(s)-1])
}

// Shape is one workload. Tasks is the total number of tasks the shape will
// execute, including children, because throughput has to be counted in tasks
// and not in submissions.
type Shape struct {
	Name  string
	Tasks int
	Run   func(sp Spawner, lat *Latency)
}

// Uniform submits tasks that all do the same amount of work.
func Uniform(tasks int, work time.Duration) Shape {
	return Shape{
		Name:  "uniform",
		Tasks: tasks,
		Run: func(sp Spawner, lat *Latency) {
			for i := 0; i < tasks; i++ {
				if lat == nil {
					sp.Submit(func() { Spin(work) })
					continue
				}
				idx, start := i, time.Now()
				sp.Submit(func() { Spin(work); lat.Record(idx, time.Since(start)) })
			}
		},
	}
}

// ForkJoin builds a binary tree of tasks in which every task submits its own
// children while it runs, which is the shape that exercises the transitive
// closure of Wait.
func ForkJoin(depth int, work time.Duration) Shape {
	tasks := (1 << (depth + 1)) - 1
	return Shape{
		Name:  "forkjoin",
		Tasks: tasks,
		Run: func(sp Spawner, _ *Latency) {
			var spawn func(d int)
			spawn = func(d int) {
				sp.Submit(func() {
					Spin(work)
					if d > 0 {
						spawn(d - 1)
						spawn(d - 1)
					}
				})
			}
			spawn(depth)
		},
	}
}

// Skewed mixes a few very long tasks into many short ones and interleaves their
// submission so neither kind clusters at one end of the queue. This is the case
// work stealing is supposed to win: a long task pins a worker while short work
// piles up behind it.
func Skewed(longs int, longWork time.Duration, shorts int, shortWork time.Duration) Shape {
	return Shape{
		Name:  "skewed",
		Tasks: longs + shorts,
		Run: func(sp Spawner, lat *Latency) {
			if longs == 0 {
				longs = 1
			}
			perLong := shorts / longs
			idx := 0
			submit := func(work time.Duration) {
				if lat == nil {
					sp.Submit(func() { Spin(work) })
					return
				}
				i, start := idx, time.Now()
				sp.Submit(func() { Spin(work); lat.Record(i, time.Since(start)) })
				idx++
			}
			for i := 0; i < longs; i++ {
				submit(longWork)
				for j := 0; j < perLong; j++ {
					submit(shortWork)
				}
			}
			// Keep Tasks honest when shorts is not divisible by longs.
			for i := longs * perLong; i < shorts; i++ {
				submit(shortWork)
			}
		},
	}
}

// Blocking submits tasks that announce a wait on a resource the scheduler
// cannot see. The baselines simply run the wait; the scheduler hands off the
// caller's queue and can grow a helper.
func Blocking(tasks int, block time.Duration) Shape {
	return Shape{
		Name:  "blocking",
		Tasks: tasks,
		Run: func(sp Spawner, _ *Latency) {
			for i := 0; i < tasks; i++ {
				sp.Submit(func() {
					sp.Block(func() { time.Sleep(block) })
				})
			}
		},
	}
}

// Naive is baseline A: one goroutine per task, unbounded. Parallelism is
// whatever the runtime gives it, so its speedup curve is bounded by
// GOMAXPROCS, not by a pool size.
type Naive struct {
	wg sync.WaitGroup
}

// NewNaive returns an idle naive runner.
func NewNaive() *Naive { return &Naive{} }

// Submit starts a goroutine for the task.
func (n *Naive) Submit(t Task) {
	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		t()
	}()
}

// Wait blocks until every submitted task has finished.
func (n *Naive) Wait() { n.wg.Wait() }

// Close is a no-op: there are no workers to stop.
func (n *Naive) Close() {}

// Block runs fn directly. The naive baseline has no scheduler to inform.
func (n *Naive) Block(fn func()) { fn() }

// Pool is baseline B: a fixed number of workers consuming one shared channel.
//
// The queue is buffered because a fork-join workload that submits children from
// inside a worker can deadlock an unbuffered channel (all workers blocked
// sending, nobody left to receive). That hazard is inherent to the design, and
// it is discussed in the README rather than designed away here.
type Pool struct {
	ch       chan Task
	workerWG sync.WaitGroup
	wg       sync.WaitGroup
	workers  int
}

// NewPool starts workers goroutines reading from a shared buffered channel.
func NewPool(workers, queue int) *Pool {
	if workers < 1 {
		workers = 1
	}
	if queue < 1 {
		queue = 1
	}
	p := &Pool{ch: make(chan Task, queue), workers: workers}
	for i := 0; i < workers; i++ {
		p.workerWG.Add(1)
		go func() {
			defer p.workerWG.Done()
			for t := range p.ch {
				t()
				p.wg.Done()
			}
		}()
	}
	return p
}

// Submit enqueues a task. Add happens before the send so Wait cannot return
// while a queued task is still outstanding.
func (p *Pool) Submit(t Task) {
	p.wg.Add(1)
	p.ch <- t
}

// Wait blocks until the queue is drained and every task has finished.
func (p *Pool) Wait() { p.wg.Wait() }

// Close stops the workers and waits for them to exit, so teardown costs the
// same accounting as the scheduler's.
func (p *Pool) Close() {
	close(p.ch)
	p.workerWG.Wait()
}

// Block runs fn directly: a channel pool has no notion of a blocked worker.
func (p *Pool) Block(fn func()) { fn() }
