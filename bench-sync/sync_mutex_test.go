package benchsync_test

import (
	"fmt"
	"sync"
	"testing"
)

type put interface {
	Put(key, value []byte) error
}

type noMutex struct {
}
type mutex struct {
	mu sync.Mutex
}
type channel struct {
	ch chan struct{}
}

var _ put = &noMutex{}
var _ put = &mutex{}

func (n *noMutex) Put(key, value []byte) error {
	// time.Sleep(n.wait)
	return nil
}

func (m *mutex) Put(key, value []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// time.Sleep(m.wait)
	return nil
}
func (m *channel) Put(key, value []byte) error {
	m.ch <- struct{}{}
	<-m.ch
	return nil
}

var benchmarks = []struct {
	batchSize int
}{
	{10},
	// {1000},
	// {10000},
	// {100000},
	// {1000000},
}

func BenchmarkNoMutex(b *testing.B) {

	for _, bm := range benchmarks {
		b.Run(fmt.Sprintf("%d", bm.batchSize), func(b *testing.B) {
			pairs := make([]struct {
				key, value []byte
			}, b.N)
			for i := 0; i < b.N; i++ {
				pairs[i].key = randomBytes(32)
				pairs[i].value = randomBytes(128)
			}

			m := &noMutex{}

			b.ResetTimer()
			n := b.N * 1000
			wg := sync.WaitGroup{}
			for i := 0; i < n; i++ {
				for j := 0; j < bm.batchSize && i < n; j++ {
					wg.Add(1)
					go func(idx int) error {
						defer wg.Done()
						return m.Put(pairs[idx/1000].key, pairs[idx/1000].value)
					}(i)
					i++
				}
				wg.Wait()
			}

		})
	}

	// goos: darwin
	// goarch: arm64
	// pkg: db/bench-sync
	// BenchmarkNoMutex/100-8           1000000                 2.212 ns/op           0 B/op          0 allocs/op
	// BenchmarkNoMutex/1000-8          1000000               221.8 ns/op             0 B/op          0 allocs/op
	// BenchmarkNoMutex/10000-8         1000000               209.7 ns/op             0 B/op          0 allocs/op
	// BenchmarkNoMutex/100000-8         1000000               214.8 ns/op             0 B/op          0 allocs/op
	// BenchmarkNoMutex/50ns-8          1000000              1306 ns/op               0 B/op          0 allocs/op
	// BenchmarkNoMutex/100ns-8         1000000              2292 ns/op               0 B/op          0 allocs/op
}

func BenchmarkMutex(b *testing.B) {
	for _, bm := range benchmarks {
		b.Run(fmt.Sprintf("%d", bm.batchSize), func(b *testing.B) {
			pairs := make([]struct {
				key, value []byte
			}, b.N)
			for i := 0; i < b.N; i++ {
				pairs[i].key = randomBytes(32)
				pairs[i].value = randomBytes(128)
			}

			m := &mutex{}

			b.ResetTimer()
			n := b.N * 1000
			wg := sync.WaitGroup{}
			for i := 0; i < n; i++ {
				for j := 0; j < bm.batchSize && i < n; j++ {
					wg.Add(1)
					go func(idx int) error {
						defer wg.Done()
						return m.Put(pairs[idx/1000].key, pairs[idx/1000].value)
					}(i)
					i++
				}
				wg.Wait()
			}
		})
	}

	// goos: darwin
	// goarch: arm64
	// pkg: db/bench-sync
	// BenchmarkNoMutex/100-8           1000000                 2.212 ns/op           0 B/op          0 allocs/op
	// BenchmarkNoMutex/1000-8          1000000               221.8 ns/op             0 B/op          0 allocs/op
	// BenchmarkNoMutex/10000-8         1000000               209.7 ns/op             0 B/op          0 allocs/op
	// BenchmarkNoMutex/100000-8         1000000               214.8 ns/op             0 B/op          0 allocs/op
	// BenchmarkNoMutex/50ns-8          1000000              1306 ns/op               0 B/op          0 allocs/op
	// BenchmarkNoMutex/100ns-8         1000000              2292 ns/op               0 B/op          0 allocs/op
}

func BenchmarkChannel(b *testing.B) {
	for _, bm := range benchmarks {
		b.Run(fmt.Sprintf("%d", bm.batchSize), func(b *testing.B) {
			pairs := make([]struct {
				key, value []byte
			}, b.N)
			for i := 0; i < b.N; i++ {
				pairs[i].key = randomBytes(32)
				pairs[i].value = randomBytes(128)
			}

			m := &channel{
				ch: make(chan struct{}, bm.batchSize),
			}

			b.ResetTimer()
			n := b.N * 1000
			for i := 0; i < n; i++ {
				wg := sync.WaitGroup{}
				for j := 0; j < bm.batchSize && i < n; j++ {
					wg.Add(1)
					go func(idx int) error {
						defer wg.Done()
						return m.Put(pairs[idx/1000].key, pairs[idx/1000].value)
					}(i)
					i++
				}
				wg.Wait()
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
