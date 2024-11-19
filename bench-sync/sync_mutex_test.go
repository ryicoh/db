package benchsync_test

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/sync/errgroup"
)

type put interface {
	Put(key, value []byte) error
}

type noMutex struct {
	wait time.Duration
}
type mutex struct {
	wait time.Duration
	mu   sync.Mutex
}
type atomicStruct struct {
	wait time.Duration
	num  atomic.Uint32
}

var _ put = &noMutex{}
var _ put = &mutex{}
var _ put = &atomicStruct{}

func (n *noMutex) Put(key, value []byte) error {
	time.Sleep(n.wait)
	return nil
}

func (m *mutex) Put(key, value []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	time.Sleep(m.wait)
	return nil
}

func (a *atomicStruct) Put(key, value []byte) error {
	a.num.Add(1)
	time.Sleep(a.wait)
	return nil
}

var benchmarks = []struct {
	wait time.Duration
	name string
}{
	{0, "0ns"},
	{1 * time.Nanosecond, "1ns"},
	{5 * time.Nanosecond, "5ns"},
	{10 * time.Nanosecond, "10ns"},
	{50 * time.Nanosecond, "50ns"},
	{100 * time.Nanosecond, "100ns"},
}

func BenchmarkNoMutex(b *testing.B) {

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			pairs := make([]struct {
				key, value []byte
			}, b.N)
			for i := 0; i < b.N; i++ {
				pairs[i].key = randomBytes(32)
				pairs[i].value = randomBytes(128)
			}

			m := &noMutex{wait: bm.wait}

			b.ResetTimer()
			for _, pair := range pairs {
				b.StartTimer()
				err := m.Put(pair.key, pair.value)
				b.StopTimer()
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}

	// goos: linux
	// goarch: amd64
	// pkg: db/bench-sync
	// cpu: Intel(R) Core(TM) i5-14500
	// BenchmarkNoMutex/0ns-20                  1000000                31.03 ns/op            0 B/op          0 allocs/op
	// BenchmarkNoMutex/1ns-20                  1000000               287.2 ns/op             0 B/op          0 allocs/op
	// BenchmarkNoMutex/5ns-20                  1000000               287.4 ns/op             0 B/op          0 allocs/op
	// BenchmarkNoMutex/10ns-20                 1000000               275.9 ns/op             0 B/op          0 allocs/op
	// BenchmarkNoMutex/50ns-20                 1000000               277.6 ns/op             0 B/op          0 allocs/op
	// BenchmarkNoMutex/100ns-20                1000000               899.3 ns/op             0 B/op          0 allocs/op
}

func BenchmarkMutex(b *testing.B) {

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			pairs := make([]struct {
				key, value []byte
			}, b.N)
			for i := 0; i < b.N; i++ {
				pairs[i].key = randomBytes(32)
				pairs[i].value = randomBytes(128)
			}

			m := &mutex{wait: bm.wait}

			eg := errgroup.Group{}
			b.ResetTimer()
			for _, pair := range pairs {
				eg.Go(func() error {
					return m.Put(pair.key, pair.value)
				})
			}
			if err := eg.Wait(); err != nil {
				b.Fatal(err)
			}
		})
	}

	// goos: linux
	// goarch: amd64
	// pkg: db/bench-sync
	// cpu: Intel(R) Core(TM) i5-14500
	// BenchmarkMutex/0ns-20                    1000000               336.7 ns/op           109 B/op          2 allocs/op
	// BenchmarkMutex/1ns-20                    1000000              1234 ns/op             380 B/op          3 allocs/op
	// BenchmarkMutex/5ns-20                    1000000               977.9 ns/op           210 B/op          3 allocs/op
	// BenchmarkMutex/10ns-20                   1000000               988.0 ns/op           205 B/op          3 allocs/op
	// BenchmarkMutex/50ns-20                   1000000              1109 ns/op             203 B/op          3 allocs/op
	// BenchmarkMutex/100ns-20                  1000000              1854 ns/op             306 B/op          3 allocs/op
}

func BenchmarkAtomic(b *testing.B) {

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			pairs := make([]struct {
				key, value []byte
			}, b.N)
			for i := 0; i < b.N; i++ {
				pairs[i].key = randomBytes(32)
				pairs[i].value = randomBytes(128)
			}

			m := &atomicStruct{wait: bm.wait}

			eg := errgroup.Group{}
			b.ResetTimer()
			for _, pair := range pairs {
				eg.Go(func() error {
					return m.Put(pair.key, pair.value)
				})
			}
			if err := eg.Wait(); err != nil {
				b.Fatal(err)
			}
			if m.num.Load() != uint32(b.N) {
				b.Fatalf("expected %d, got %d", b.N, m.num.Load())
			}
		})
	}

	// goos: linux
	// goarch: amd64
	// pkg: db/bench-sync
	// cpu: Intel(R) Core(TM) i5-14500
	// BenchmarkMutex/0ns-20                    1000000               336.7 ns/op           109 B/op          2 allocs/op
	// BenchmarkMutex/1ns-20                    1000000              1234 ns/op             380 B/op          3 allocs/op
	// BenchmarkMutex/5ns-20                    1000000               977.9 ns/op           210 B/op          3 allocs/op
	// BenchmarkMutex/10ns-20                   1000000               988.0 ns/op           205 B/op          3 allocs/op
	// BenchmarkMutex/50ns-20                   1000000              1109 ns/op             203 B/op          3 allocs/op
	// BenchmarkMutex/100ns-20                  1000000              1854 ns/op             306 B/op          3 allocs/op
}
