package benchsync_test

import (
	"fmt"
	"testing"
	"time"
)

type Something interface {
	Do()
}

type syncSomething struct {
	wait time.Duration
}

var _ Something = &syncSomething{}

func (s *syncSomething) Do() {
	time.Sleep(s.wait)
}

type asyncSomething struct {
	wait time.Duration
	ch   chan chan struct{}
}

var _ Something = &asyncSomething{}

func newAsyncSomething(wait time.Duration, chanCap int) *asyncSomething {
	return &asyncSomething{
		wait: wait,
		ch:   make(chan chan struct{}, chanCap),
	}
}

func (s *asyncSomething) StartWorker() {
	go func() {
		for done := range s.ch {
			time.Sleep(s.wait)
			done <- struct{}{}
		}
	}()
}

func (s *asyncSomething) Do() {
	done := make(chan struct{})
	s.ch <- done
	<-done
}

func BenchmarkChannelSync(b *testing.B) {
	benchmarks := []struct {
		wait time.Duration
	}{
		{10 * time.Microsecond},
		{100 * time.Microsecond},
		{1000 * time.Microsecond},
	}
	for _, bm := range benchmarks {
		b.Run(fmt.Sprintf("wait %s", bm.wait), func(b *testing.B) {
			s := &syncSomething{wait: bm.wait}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				s.Do()
			}
		})
	}
}

func BenchmarkChannelAsync(b *testing.B) {
	benchmarks := []struct {
		chanCap int
		wait    time.Duration
	}{
		{1, 10 * time.Microsecond},
		{10, 10 * time.Microsecond},
		{100, 10 * time.Microsecond},
		{1, 100 * time.Microsecond},
		{10, 100 * time.Microsecond},
		{100, 100 * time.Microsecond},
		{1, 1000 * time.Microsecond},
		{10, 1000 * time.Microsecond},
		{100, 1000 * time.Microsecond},
	}

	for _, bm := range benchmarks {
		b.Run(fmt.Sprintf("chan cap %d", bm.chanCap), func(b *testing.B) {
			s := newAsyncSomething(bm.wait, bm.chanCap)
			s.StartWorker()

			for i := 0; i < b.N; i++ {
				s.Do()
			}
		})
	}
}
