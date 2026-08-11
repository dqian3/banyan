package replica

import "time"

// setDuration writes v at index i, growing the slice as needed, and returns
// the (possibly reallocated) slice.
//
// These per-height stat arrays were fixed at 10,000 entries, so any run whose
// chain passed height 10,000 died with "index out of range [10000] with
// length 10000" — every node at once, since they all reach that height
// together. It bites easily under the client workload, where an empty mempool
// still produces blocks and empty blocks commit as fast as the network
// allows. Growth is amortized doubling, so a long run costs a handful of
// reallocations rather than a crash.
func setDuration(s []time.Duration, i int, v time.Duration) []time.Duration {
	if i < 0 {
		return s
	}
	if i >= len(s) {
		grown := make([]time.Duration, max(i+1, 2*len(s)))
		copy(grown, s)
		s = grown
	}
	s[i] = v
	return s
}

// max is spelled out because the toolchain is pinned to Go 1.19, which
// predates the builtin.
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
