package benchsync_test

import (
	"bytes"
	"context"
	benchsync "db/bench-sync"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
)

func TestDBGroupCommit4(t *testing.T) {
	numPairs := int(1e5)
	fmt.Println("numPairs:", numPairs)
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
		// {1 << 10, 1 << 5, 1 << 0},
		// {1 << 10, 1 << 10, 1 << 0},
		// {1 << 10, 1 << 10, 1 << 10},
		// {1 << 10, 1 << 15, 1 << 10},
		// {1 << 10, 1 << 15, 1 << 15},

		// {1 << 15, 1 << 5, 1 << 0},
		// {1 << 15, 1 << 10, 1 << 0},
		// {1 << 15, 1 << 10, 1 << 10},
		// {1 << 15, 1 << 15, 1 << 10},
		{1 << 20, 1 << 18, 1 << 4},
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

			var stats runtime.MemStats
			runtime.ReadMemStats(&stats)
			b.Logf("b.N: %d, db: %s, mem: %.2fMB\n", b.N, db.Stats(), float64(stats.Alloc)/1e6)
		})
	}

	// goos: linux
	// goarch: amd64
	// pkg: db/bench-sync
	// cpu: Intel(R) Core(TM) i5-14500
	// BenchmarkDBGroupCommit4/semaphoreWeight1048576_putQueueCapacity262144_writeQueueCapacity16-20           10897962               954.9 ns/op          4612 B/op          4 allocs/op
	// --- BENCH: BenchmarkDBGroupCommit4/semaphoreWeight1048576_putQueueCapacity262144_writeQueueCapacity16-20
	// 	db_5_group_commit4_test.go:133: b.N: 1, db: put:1|0/262144, write:1|0/16, notify:0/32, write:10.457905ms, interval:0s, mem: 32.95MB
	// 	db_5_group_commit4_test.go:133: b.N: 100, db: put:100|99/262144, write:1|0/16, notify:0/32, write:3.037631ms, interval:0s, mem: 33.11MB
	// 	db_5_group_commit4_test.go:133: b.N: 10000, db: put:10000|1436/262144, write:6|0/16, notify:0/32, write:1.00607ms, interval:1.349158ms, mem: 62.38MB
	// 	db_5_group_commit4_test.go:133: b.N: 1000000, db: put:1000000|4179/262144, write:190|1/16, notify:0/32, write:2.000418ms, interval:3.794097ms, mem: 169.29MB
	// 	db_5_group_commit4_test.go:133: b.N: 10897962, db: put:10897962|3438/262144, write:2608|2/16, notify:2/32, write:1.571448ms, interval:2.418201ms, mem: 737.39MB
	// PASS
	// ok      db/bench-sync   34.802s

	// 1048218 op/s
}
