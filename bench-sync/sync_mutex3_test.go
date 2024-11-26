package benchsync_test

import (
	"sync"
	"testing"
)

type noMutex2 struct {
	data [][]byte
}
type mutex2 struct {
	data [][]byte
	mu   sync.Mutex
}
type chanData struct {
	data []byte
	done chan struct{}
}
type chan2 struct {
	data chan *chanData
}

var _ put = &noMutex2{}
var _ put = &mutex2{}
var _ put = &chan2{}

const maxDataLen = 1000000

func (n *noMutex2) Put(key, value []byte) error {
	if len(n.data) > maxDataLen {
		n.data = make([][]byte, 0, maxDataLen)
		// Do something to the data
	}
	n.data = append(n.data, value)
	return nil
}

func (m *mutex2) Put(key, value []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if len(m.data) > maxDataLen {
		m.data = make([][]byte, 0, maxDataLen)
		// Do something to the data
	}
	m.data = append(m.data, value)

	return nil
}
func (c *chan2) Start() {
	go func() {
		for value := range c.data {
			data := make([][]byte, 0, maxDataLen)
			chans := make([]chan struct{}, 0, maxDataLen)
			data = append(data, value.data)
			chans = append(chans, value.done)
			for len(c.data) > 0 && len(data) < maxDataLen {
				value := <-c.data
				data = append(data, value.data)
				chans = append(chans, value.done)
			}

			for _, done := range chans {
				done <- struct{}{}
			}
			// Do something to the data
		}
	}()
}
func (c *chan2) Put(key, value []byte) error {
	done := make(chan struct{}, 1)
	c.data <- &chanData{data: value, done: done}
	<-done
	return nil
}

func BenchmarkNoMutex2(b *testing.B) {
	pairs := make([]struct {
		key, value []byte
	}, b.N)
	for i := 0; i < b.N; i++ {
		pairs[i].key = randomBytes(32)
		pairs[i].value = randomBytes(128)
	}

	m := &noMutex2{}
	const n = 1000

	b.ResetTimer()
	for _, pair := range pairs {
		for i := 0; i < n; i++ {
			err := m.Put(pair.key, pair.value)
			if err != nil {
				b.Fatal(err)
			}
		}
	}
}
func BenchmarkMutex2(b *testing.B) {
	pairs := make([]struct {
		key, value []byte
	}, b.N)
	for i := 0; i < b.N; i++ {
		pairs[i].key = randomBytes(32)
		pairs[i].value = randomBytes(128)
	}

	m := &mutex2{}
	const n = 1000

	var wg sync.WaitGroup
	b.ResetTimer()
	for _, pair := range pairs {
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				m.Put(pair.key, pair.value)
			}()
		}
	}
	wg.Wait()
}
func BenchmarkChan2(b *testing.B) {
	pairs := make([]struct {
		key, value []byte
	}, b.N)
	for i := 0; i < b.N; i++ {
		pairs[i].key = randomBytes(32)
		pairs[i].value = randomBytes(128)
	}

	m := &chan2{
		data: make(chan *chanData, 1000),
	}
	m.Start()
	const n = 1000

	var wg sync.WaitGroup
	b.ResetTimer()
	for _, pair := range pairs {
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				m.Put(pair.key, pair.value)
			}()
		}
	}
	wg.Wait()
}
