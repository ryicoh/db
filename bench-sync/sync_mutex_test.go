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
	workers      int
	workerChSize int
}{
	{1000, 10},
	{1000, 100},
	{10000, 10},
	{10000, 100},
	{100000, 10},
	{100000, 100},
	{1000000, 10},
	{1000000, 100},
}

type pair struct {
	key, value []byte
}

func BenchmarkNoMutex(b *testing.B) {
	for _, bm := range benchmarks {
		b.Run(fmt.Sprintf("workers=%d_workerChSize=%d", bm.workers, bm.workerChSize), func(b *testing.B) {
			pairs := make([]pair, b.N)
			for i := 0; i < b.N; i++ {
				pairs[i].key = randomBytes(32)
				pairs[i].value = randomBytes(128)
			}

			m := &noMutex{}
			wg := sync.WaitGroup{}

			workerChs := make([]chan pair, bm.workers)
			for i := 0; i < bm.workers; i++ {
				workerChs[i] = make(chan pair, bm.workerChSize)
				go func(i int) {
					for p := range workerChs[i] {
						m.Put(p.key, p.value)
						wg.Done()
					}
				}(i)
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				wg.Add(1)
				workerChs[i%bm.workers] <- pairs[i]
			}
			wg.Wait()

			b.StopTimer()
			for _, ch := range workerChs {
				close(ch)
			}
		})
	}

	// goos: linux
	// goarch: amd64
	// pkg: db/bench-sync
	// cpu: Intel(R) Core(TM) i5-14500
	// BenchmarkNoMutex/workers=1000_workerChSize=10-20                 1000000               132.5 ns/op             0 B/op          0 allocs/op
	// BenchmarkNoMutex/workers=1000_workerChSize=100-20                1000000               118.8 ns/op             0 B/op          0 allocs/op
	// BenchmarkNoMutex/workers=10000_workerChSize=10-20                1000000               128.8 ns/op             0 B/op          0 allocs/op
	// BenchmarkNoMutex/workers=10000_workerChSize=100-20               1000000               135.1 ns/op             0 B/op          0 allocs/op
	// BenchmarkNoMutex/workers=100000_workerChSize=10-20               1000000               150.8 ns/op             0 B/op          0 allocs/op
	// BenchmarkNoMutex/workers=100000_workerChSize=100-20              1000000               158.1 ns/op             0 B/op          0 allocs/op
	// BenchmarkNoMutex/workers=1000000_workerChSize=10-20              1000000               158.1 ns/op             0 B/op          0 allocs/op
	// BenchmarkNoMutex/workers=1000000_workerChSize=100-20             1000000               152.8 ns/op             0 B/op          0 allocs/op
	// PASS
	// ok      db/bench-sync   24.357s
}

func BenchmarkMutex(b *testing.B) {
	for _, bm := range benchmarks {
		b.Run(fmt.Sprintf("workers=%d_workerChSize=%d", bm.workers, bm.workerChSize), func(b *testing.B) {
			pairs := make([]struct {
				key, value []byte
			}, b.N)
			for i := 0; i < b.N; i++ {
				pairs[i].key = randomBytes(32)
				pairs[i].value = randomBytes(128)
			}

			m := &mutex{}
			wg := sync.WaitGroup{}

			workerChs := make([]chan pair, bm.workers)
			for i := 0; i < bm.workers; i++ {
				workerChs[i] = make(chan pair, bm.workerChSize)
				go func(i int) {
					for p := range workerChs[i] {
						m.Put(p.key, p.value)
						wg.Done()
					}
				}(i)
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				wg.Add(1)
				workerChs[i%bm.workers] <- pairs[i]
			}
			wg.Wait()

			b.StopTimer()
			for _, ch := range workerChs {
				close(ch)
			}
		})
	}

	// goos: linux
	// goarch: amd64
	// pkg: db/bench-sync
	// cpu: Intel(R) Core(TM) i5-14500
	// BenchmarkMutex/workers=1000_workerChSize=10-20           1000000               332.8 ns/op             0 B/op          0 allocs/op
	// BenchmarkMutex/workers=1000_workerChSize=100-20          1000000               278.5 ns/op             0 B/op          0 allocs/op
	// BenchmarkMutex/workers=10000_workerChSize=10-20          1000000               306.1 ns/op             0 B/op          0 allocs/op
	// BenchmarkMutex/workers=10000_workerChSize=100-20         1000000               135.6 ns/op             0 B/op          0 allocs/op
	// BenchmarkMutex/workers=100000_workerChSize=10-20         1000000               250.4 ns/op             0 B/op          0 allocs/op
	// BenchmarkMutex/workers=100000_workerChSize=100-20        1000000               257.8 ns/op             0 B/op          0 allocs/op
	// BenchmarkMutex/workers=1000000_workerChSize=10-20        1000000               363.7 ns/op             0 B/op          0 allocs/op
	// BenchmarkMutex/workers=1000000_workerChSize=100-20       1000000               293.8 ns/op             0 B/op          0 allocs/op
	// PASS
	// ok      db/bench-sync   26.359s
}

func BenchmarkChannel(b *testing.B) {
	for _, bm := range benchmarks {
		b.Run(fmt.Sprintf("workers=%d_workerChSize=%d", bm.workers, bm.workerChSize), func(b *testing.B) {
			pairs := make([]struct {
				key, value []byte
			}, b.N)
			for i := 0; i < b.N; i++ {
				pairs[i].key = randomBytes(32)
				pairs[i].value = randomBytes(128)
			}

			m := &channel{
				ch: make(chan struct{}, 8),
			}
			wg := sync.WaitGroup{}

			workerChs := make([]chan pair, bm.workers)
			for i := 0; i < bm.workers; i++ {
				workerChs[i] = make(chan pair, bm.workerChSize)
				go func(i int) {
					for p := range workerChs[i] {
						m.Put(p.key, p.value)
						wg.Done()
					}
				}(i)
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				wg.Add(1)
				workerChs[i%bm.workers] <- pairs[i]
			}
			wg.Wait()

			b.StopTimer()
			for _, ch := range workerChs {
				close(ch)
			}
		})
	}

	// goos: linux
	// goarch: amd64
	// pkg: db/bench-sync
	// cpu: Intel(R) Core(TM) i5-14500
	// BenchmarkChannel/workers=1000_workerChSize=10-20                 1000000               432.0 ns/op             0 B/op          0 allocs/op
	// BenchmarkChannel/workers=1000_workerChSize=100-20                1000000               451.9 ns/op             0 B/op          0 allocs/op
	// BenchmarkChannel/workers=10000_workerChSize=10-20                1000000               456.9 ns/op             0 B/op          0 allocs/op
	// BenchmarkChannel/workers=10000_workerChSize=100-20               1000000               455.0 ns/op             0 B/op          0 allocs/op
	// BenchmarkChannel/workers=100000_workerChSize=10-20               1000000               501.9 ns/op             0 B/op          0 allocs/op
	// BenchmarkChannel/workers=100000_workerChSize=100-20              1000000               512.3 ns/op             0 B/op          0 allocs/op
	// BenchmarkChannel/workers=1000000_workerChSize=10-20              1000000               596.3 ns/op             0 B/op          0 allocs/op
	// BenchmarkChannel/workers=1000000_workerChSize=100-20             1000000               609.4 ns/op             0 B/op          0 allocs/op
	// PASS
	// ok      db/bench-sync   27.579s
}
