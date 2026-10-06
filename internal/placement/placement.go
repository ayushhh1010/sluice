// Package placement maps partitions and groups to agents with rendezvous
// hashing. Ownership is only a routing hint for cache locality: offsets come
// from the sequencer, so two agents briefly disagreeing cannot corrupt data.
package placement

// Owner returns the node with the highest score for (topic, partition), or
// -1 when nodes is empty.
func Owner(nodes []int32, topic [16]byte, partition uint32) int32 {
	h := fnv(topic[:])
	h = mix(h ^ uint64(partition))
	return pick(nodes, h)
}

// Coordinator returns the node coordinating a consumer group.
func Coordinator(nodes []int32, group string) int32 {
	return pick(nodes, fnv([]byte(group)))
}

func pick(nodes []int32, h uint64) int32 {
	best, bestScore := int32(-1), uint64(0)
	for _, n := range nodes {
		s := mix(h ^ mix(uint64(uint32(n))))
		if best == -1 || s > bestScore || (s == bestScore && n < best) {
			best, bestScore = n, s
		}
	}
	return best
}

func fnv(b []byte) uint64 {
	h := uint64(14695981039346656037)
	for _, c := range b {
		h ^= uint64(c)
		h *= 1099511628211
	}
	return h
}

// mix is the splitmix64 finalizer.
func mix(z uint64) uint64 {
	z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
	z = (z ^ (z >> 27)) * 0x94d049bb133111eb
	return z ^ (z >> 31)
}
