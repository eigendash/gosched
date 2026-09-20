package sched

import (
	"sync"
	"sync/atomic"
	"testing"
)

// BenchmarkDequePushPop measures the owner's hot path: the depth-first end that
// every worker uses for its own tasks.
func BenchmarkDequePushPop(b *testing.B) {
	d := NewDeque()
	t := func() {}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		d.Push(t)
		if _, ok := d.Pop(); !ok {
			b.Fatal("pop of a non-empty deque failed")
		}
	}
}

// BenchmarkDequeStealContended measures the thief end with four goroutines
// fighting over the same deque. The number is aggregate steal latency under
// contention, which is the case that matters when the pool is unbalanced.
func BenchmarkDequeStealContended(b *testing.B) {
	const thieves = 4
	d := NewDeque()
	t := func() {}
	var stolen int64
	stop := make(chan struct{})

	// Keep the deque fed so thieves always have something to race for.
	var producerDone sync.WaitGroup
	producerDone.Add(1)
	go func() {
		defer producerDone.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			d.Push(t)
		}
	}()

	var wg sync.WaitGroup
	for i := 0; i < thieves; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				if _, ok := d.Steal(); ok {
					atomic.AddInt64(&stolen, 1)
					continue
				}
				select {
				case <-stop:
					return
				default:
				}
			}
		}()
	}

	deadline := int64(b.N)
	for atomic.LoadInt64(&stolen) < deadline {
		// Spin until the thieves have collectively stolen b.N entries.
	}
	close(stop)
	producerDone.Wait()
	wg.Wait()
}
