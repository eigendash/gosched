package sched

import "runtime"

// goid returns the current goroutine's id, or 0 if the runtime's stack header
// could not be parsed.
//
// This exists for exactly one caller: Blocking needs to know which worker owns
// the goroutine that is about to wait, and Go deliberately offers no
// goroutine-local storage. The alternatives were worse:
//
//   - Changing Task to func(*Worker) would push scheduler plumbing into every
//     task signature and every closure that submits children.
//   - //go:linkname runtime.getg is rejected by the toolchain's linkname policy.
//   - runtime/pprof goroutine labels are writable but reading them back needs
//     the profiling machinery, and they are not documented as a data channel.
//
// The cost is real — a few hundred nanoseconds, dominated by formatting the
// stack header — so it is confined to Blocking, which already implies waiting
// on a syscall or another task. Submit, the hot path, never calls it: the
// measured price of tracing on the submit path is in results/ and the README.
func goid() uint64 {
	var buf [32]byte
	n := runtime.Stack(buf[:], false)
	const prefix = "goroutine "
	if n < len(prefix) {
		return 0
	}
	for i := 0; i < len(prefix); i++ {
		if buf[i] != prefix[i] {
			return 0
		}
	}
	var id uint64
	digits := 0
	for i := len(prefix); i < n; i++ {
		c := buf[i]
		if c < '0' || c > '9' {
			break
		}
		id = id*10 + uint64(c-'0')
		digits++
	}
	if digits == 0 {
		return 0
	}
	return id
}
