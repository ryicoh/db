package benchsync_test

import (
	"bytes"
	"context"
	benchsync "db/bench-sync"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
)

func TestDBGroupCommit4(t *testing.T) {
	numPairs := 200
	pairs := make([]struct {
		key, value []byte
	}, numPairs)
	for i := 0; i < numPairs; i++ {
		pairs[i].key = []byte(fmt.Sprintf("key%03d", i))
		pairs[i].value = []byte(fmt.Sprintf("value%03d", i))
	}

	cfg := benchsync.DBGroupCommit4Config{
		PutQueueCapacity:    1024,
		WriteQueueCapacity:  1024,
		NotifyQueueCapacity: 1024,
	}
	db, err := benchsync.OpenDBGroupCommit4(filepath.Join(testDir, t.Name()), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	db.StartWorkers(context.TODO())
	sem := semaphore.NewWeighted(10000)
	var eg errgroup.Group
	for _, pair := range pairs {
		eg.Go(func() error {
			if err := sem.Acquire(context.TODO(), 1); err != nil {
				return err
			}
			defer sem.Release(1)

			return db.Put(pair.key, pair.value)
		})
	}

	if err := eg.Wait(); err != nil {
		t.Fatal(err)
	}

	for _, pair := range pairs {
		value, err := db.Get(pair.key)
		if err != nil {
			t.Fatalf("get key %s: %s", pair.key, err)
		}
		if !bytes.Equal(value, pair.value) {
			t.Fatalf("value mismatch: %s != %s", value, pair.value)
		}
	}
}

func BenchmarkDBGroupCommit4(b *testing.B) {
	os.MkdirAll(filepath.Join(testDir, b.Name()), 0o755)

	benchmarks := []struct {
		semaphoreWeight    int64
		putQueueCapacity   int
		writeQueueCapacity int
	}{
		{1 << 10, 1 << 5, 1 << 0},
		{1 << 10, 1 << 5, 1 << 0},
		{1 << 10, 1 << 10, 1 << 0},
		{1 << 10, 1 << 10, 1 << 0},
		{1 << 10, 1 << 10, 1 << 10},
		{1 << 10, 1 << 10, 1 << 10},
		{1 << 10, 1 << 20, 1 << 10},
		{1 << 10, 1 << 20, 1 << 10},

		{1 << 20, 1 << 5, 1 << 0},
		{1 << 20, 1 << 5, 1 << 0},
		{1 << 20, 1 << 10, 1 << 0},
		{1 << 20, 1 << 10, 1 << 0},
		{1 << 20, 1 << 10, 1 << 10},
		{1 << 20, 1 << 10, 1 << 10},
		{1 << 20, 1 << 20, 1 << 10},
		{1 << 20, 1 << 20, 1 << 10},
	}

	for _, bm := range benchmarks {
		b.Run(fmt.Sprintf("semaphoreWeight%d_putQueueCapacity%d_writeQueueCapacity%d", bm.semaphoreWeight, bm.putQueueCapacity, bm.writeQueueCapacity), func(b *testing.B) {
			pairs := make([]struct {
				key, value []byte
			}, b.N)
			for i := 0; i < b.N; i++ {
				pairs[i].key = randomBytes(32)
				pairs[i].value = randomBytes(128)
			}

			cfg := benchsync.DBGroupCommit4Config{
				PutQueueCapacity:    bm.putQueueCapacity,
				WriteQueueCapacity:  bm.writeQueueCapacity,
				NotifyQueueCapacity: 32,
			}
			db, err := benchsync.OpenDBGroupCommit4(filepath.Join(testDir, b.Name()), cfg)
			if err != nil {
				b.Fatal(err)
			}
			defer db.Close()

			db.StartWorkers(context.TODO())

			var eg errgroup.Group
			sem := semaphore.NewWeighted(bm.semaphoreWeight)
			b.ResetTimer()
			for _, pair := range pairs {
				eg.Go(func() error {
					if err := sem.Acquire(context.TODO(), 1); err != nil {
						return err
					}
					defer sem.Release(1)

					return db.Put(pair.key, pair.value)
				})
			}
			if err := eg.Wait(); err != nil {
				b.Fatal(err)
			}

			b.StopTimer()

			b.Logf("b.N: %d, stats: %s\n", b.N, db.Stats())
		})
	}
}
