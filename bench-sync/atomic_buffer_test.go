package benchsync_test

import (
	"sync"
	"sync/atomic"
	"testing"

	"golang.org/x/sync/errgroup"
)

type buffer interface {
	Append(value []byte)
	Flush() [][]byte
}

type syncMutexBuffer struct {
	buf [][]byte
	mu  sync.Mutex
}
type atomicBuffer struct {
	buf   [][]byte
	index atomic.Uint64
	size  atomic.Uint64
}

var _ buffer = &syncMutexBuffer{}
var _ buffer = &atomicBuffer{}

func (n *syncMutexBuffer) Append(value []byte) {
	n.mu.Lock()
	n.buf = append(n.buf, value)
	n.mu.Unlock()
}

func (m *syncMutexBuffer) Flush() [][]byte {
	m.mu.Lock()
	buf := m.buf
	m.buf = make([][]byte, 0, len(buf)/2)
	m.mu.Unlock()

	return buf
}

func (a *atomicBuffer) Append(value []byte) {
	for {
		index := a.index.Load()
		if a.index.CompareAndSwap(index, index+1) {
			a.buf[index] = value
			a.size.Add(1)
			return
		}
	}
}

func (a *atomicBuffer) Flush() [][]byte {
	size := a.size.Load()
	data := make([][]byte, size)
	for i := uint64(0); i < size; i++ {
		data[i] = a.buf[i]
	}
	a.index.Store(0)
	a.size.Store(0)
	return data
}

func BenchmarkBufferSyncMutex(b *testing.B) {
	pairs := make([]struct {
		value []byte
	}, b.N)
	for i := 0; i < b.N; i++ {
		pairs[i].value = randomBytes(128)
	}

	m := &syncMutexBuffer{}
	result := map[string]struct{}{}

	eg := errgroup.Group{}
	mu := sync.Mutex{}

	b.ResetTimer()
	for i, pair := range pairs {
		i := i
		eg.Go(func() error {
			m.Append(pair.value)
			if i%1000 == 0 {
				flushed := m.Flush()

				mu.Lock()
				for _, v := range flushed {
					result[string(v)] = struct{}{}
				}
				mu.Unlock()
			}
			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		b.Fatal(err)
	}
	flushed := m.Flush()
	for _, v := range flushed {
		result[string(v)] = struct{}{}
	}
	if len(result) != b.N {
		b.Fatalf("expected %d, got %d", b.N, len(result))
	}

	for _, pair := range pairs {
		if _, ok := result[string(pair.value)]; !ok {
			b.Fatalf("expected %s, got %v", pair.value, result)
		}
	}
}

func BenchmarkBufferAtomic(b *testing.B) {
	pairs := make([]struct {
		value []byte
	}, b.N)
	for i := 0; i < b.N; i++ {
		pairs[i].value = randomBytes(128)
	}

	m := &atomicBuffer{buf: make([][]byte, b.N)}
	result := map[string]struct{}{}

	eg := errgroup.Group{}
	mux := sync.Mutex{}

	b.ResetTimer()
	for i, pair := range pairs {
		eg.Go(func() error {
			m.Append(pair.value)
			if i%1000 == 0 {
				flushed := m.Flush()

				mux.Lock()
				for _, v := range flushed {
					result[string(v)] = struct{}{}
				}
				mux.Unlock()
			}
			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		b.Fatal(err)
	}

	flushed := m.Flush()
	for _, v := range flushed {
		result[string(v)] = struct{}{}
	}

	if len(result) != b.N {
		b.Fatalf("expected %d, got %d", b.N, len(result))
	}

	for _, pair := range pairs {
		if _, ok := result[string(pair.value)]; !ok {
			b.Fatalf("expected %s, got %v", pair.value, result)
		}
	}
}
