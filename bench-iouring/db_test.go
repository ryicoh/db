package benchiouring_test

import (
	"bytes"
	"context"
	benchiouring "db/bench-iouring"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/montanaflynn/stats"
	"golang.org/x/sync/errgroup"
)

const testDir = "testdata"

func TestDB(t *testing.T) {
	fileName := filepath.Join(testDir, t.Name())
	os.RemoveAll(fileName)

	numPairs := int(1e7)
	pairs := make([]struct {
		key, value []byte
	}, numPairs)
	for i := 0; i < numPairs; i++ {
		pairs[i].key = []byte(fmt.Sprintf("key%03d", i))
		pairs[i].value = []byte(fmt.Sprintf("value%03d", i))
	}

	cfg := benchiouring.DBConfig{
		Entries:         1,
		PutQueueSize:    102400,
		SubmitQueueSize: 0,
		MaxBatchSize:    1 << 30, // 1GB
	}
	db, err := benchiouring.OpenDB(fileName, cfg)
	if err != nil {
		t.Fatal(err)
	}

	db.Start(context.TODO())
	var eg errgroup.Group
	for _, pair := range pairs {
		eg.Go(func() error {
			return db.Put(pair.key, pair.value)
		})
	}

	if err := eg.Wait(); err != nil {
		t.Fatal(err)
	}

	fmt.Println(db.Stats())

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

func BenchmarkDB(b *testing.B) {
	os.MkdirAll(filepath.Join(testDir, b.Name()), 0o755)

	benchmarks := []struct {
		putQueueSize    int
		submitQueueSize int
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
		{1 << 20, 1 << 1},
	}

	key := []byte("key1234567890")
	value := []byte("value12345678901234567890")

	for _, bm := range benchmarks {
		b.Run(fmt.Sprintf("putQueueSize%d_submitQueueSize%d", bm.putQueueSize, bm.submitQueueSize), func(b *testing.B) {
			pairs := make([]struct {
				key, value []byte
			}, b.N)
			for i := 0; i < b.N; i++ {
				pairs[i].key = key
				pairs[i].value = value
			}

			cfg := benchiouring.DBConfig{
				Entries:         64,
				PutQueueSize:    bm.putQueueSize,
				SubmitQueueSize: bm.submitQueueSize,
				MaxBatchSize:    1 << 30, // 1GB
			}
			db, err := benchiouring.OpenDB(filepath.Join(testDir, b.Name()), cfg)
			if err != nil {
				b.Fatal(err)
			}

			db.Start(context.TODO())

			durations := make([]float64, 0, b.N)
			durationMux := sync.Mutex{}

			var wg sync.WaitGroup
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					start := time.Now()
					db.Put(key, value)

					durationMux.Lock()
					defer durationMux.Unlock()
					durations = append(durations, float64(time.Since(start).Milliseconds()))
				}()
			}

			wg.Wait()
			b.StopTimer()

			average, err := stats.Mean(stats.Float64Data(durations))
			if err != nil {
				b.Fatal(err)
			}
			relency, err := stats.Percentile(stats.Float64Data(durations), 90)
			if err != nil {
				b.Fatal(err)
			}

			var stats runtime.MemStats
			runtime.ReadMemStats(&stats)

			b.Logf("b.N: %d, db: %s, mem: %.2fMB, average: %.2fms, relency: %.2fms\n", b.N, db.Stats(), float64(stats.Alloc)/1e6, average, relency)
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
