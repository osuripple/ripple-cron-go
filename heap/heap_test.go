package main

import (
	"container/heap"
	"math/rand"
	"sort"
	"testing"
)

// Float64Heap is a max heap of float64s
type Float64Heap []float64

func (h Float64Heap) Len() int           { return len(h) }
func (h Float64Heap) Less(i, j int) bool { return h[i] > h[j] }
func (h Float64Heap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *Float64Heap) Push(x interface{}) {
	// Push and Pop use pointer receivers because they modify the slice's length,
	// not just its contents.
	*h = append(*h, x.(float64))
}

func (h *Float64Heap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

func benchmarkHeap(n int, b *testing.B) {
	rand.Seed(1337)
	h := &Float64Heap{}
	// No need to heapify
	// heap.Init(h)
	for j := 0; j < b.N; j++ {
		for i := 0; i < n; i++ {
			heap.Push(h, rand.Float64()*1000)
		}
		for h.Len() > 0 {
			heap.Pop(h)
			// fmt.Printf("%f\n", heap.Pop(h))
		}
	}
}

func Insert(ss []float64, s float64) []float64 {
	i := sort.Search(len(ss), func(i int) bool { return ss[i] <= s })
	ss = append(ss, 0.)
	copy(ss[i+1:], ss[i:])
	ss[i] = s
	return ss
}

func benchmarkSlice(n int, b *testing.B) {
	rand.Seed(1337)
	for j := 0; j < b.N; j++ {
		var slice []float64
		for i := 0; i < n; i++ {
			slice = Insert(slice, rand.Float64()*1000)
		}
		for range slice {
			// fmt.Printf("%f\n", v)
		}
	}
}

func BenchmarkHeap10(b *testing.B)      { benchmarkHeap(10, b) }
func BenchmarkHeap100(b *testing.B)     { benchmarkHeap(100, b) }
func BenchmarkHeap1000(b *testing.B)    { benchmarkHeap(1000, b) }
func BenchmarkHeap10000(b *testing.B)   { benchmarkHeap(10000, b) }
func BenchmarkHeap100000(b *testing.B)  { benchmarkHeap(100000, b) }
func BenchmarkHeap1000000(b *testing.B) { benchmarkHeap(1000000, b) }

func BenchmarkSlice10(b *testing.B)      { benchmarkSlice(10, b) }
func BenchmarkSlice100(b *testing.B)     { benchmarkSlice(100, b) }
func BenchmarkSlice1000(b *testing.B)    { benchmarkSlice(1000, b) }
func BenchmarkSlice10000(b *testing.B)   { benchmarkSlice(10000, b) }
func BenchmarkSlice100000(b *testing.B)  { benchmarkSlice(100000, b) }
func BenchmarkSlice1000000(b *testing.B) { benchmarkSlice(1000000, b) }
