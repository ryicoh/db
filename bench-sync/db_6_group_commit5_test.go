package benchsync_test

import (
	"bytes"
	"context"
	benchsync "db/bench-sync"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"

	"golang.org/x/sync/errgroup"
)

func TestUnsafeConvert(t *testing.T) {
	meta := benchsync.Meta{
		KeyLen:   uint32(len([]byte("key"))),
		ValueLen: uint32(len([]byte("value"))),
	}
	buf1 := benchsync.UnsafeMetaToBuffer(&meta)
	fmt.Println("buf1", buf1)
	buf2 := make([]byte, len(buf1))
	copy(buf2, buf1)
	item2 := benchsync.UnsafeBufferToMeta(buf2)
	if !reflect.DeepEqual(meta, *item2) {
		t.Fatalf("meta mismatch: %v != %v", meta, *item2)
	}
}

func TestDBGroupCommit5(t *testing.T) {
	numPairs := int(1)
	pairs := make([]struct {
		key, value []byte
	}, numPairs)

	for i := 0; i < numPairs; i++ {
		pairs[i].key = []byte(fmt.Sprintf("key%03d", i))
		pairs[i].value = []byte(fmt.Sprintf("value%03d", i))
	}

	cfg := benchsync.DBGroupCommit5Config{
		PutQueueCapacity:    1024,
		WriteQueueCapacity:  1024,
		NotifyQueueCapacity: 1024,
	}
	db, err := benchsync.OpenDBGroupCommit5(filepath.Join(testDir, t.Name()), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	db.StartWorkers(context.TODO())
	var eg errgroup.Group
	for _, pair := range pairs {
		eg.Go(func() error {
			return db.Put(&benchsync.Item{
				Key:   pair.key,
				Value: pair.value,
			})
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

func BenchmarkDBGroupCommit5(b *testing.B) {
	os.MkdirAll(filepath.Join(testDir, b.Name()), 0o755)

	benchmarks := []struct {
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
		{1 << 16, 1 << 4},
	}
	key := randomBytes(32)
	value := randomBytes(256)

	allSize := atomic.Uint64{}

	for _, bm := range benchmarks {
		b.Run(fmt.Sprintf("putQueueCapacity%d_writeQueueCapacity%d", bm.putQueueCapacity, bm.writeQueueCapacity), func(b *testing.B) {
			cfg := benchsync.DBGroupCommit5Config{
				PutQueueCapacity:    bm.putQueueCapacity,
				MaxBufferLen:        1 << 28, // 256MB
				WriteQueueCapacity:  bm.writeQueueCapacity,
				NotifyQueueCapacity: 256,
			}
			db, err := benchsync.OpenDBGroupCommit5(filepath.Join(testDir, b.Name()), cfg)
			if err != nil {
				b.Fatal(err)
			}
			defer db.Close()

			db.StartWorkers(context.TODO())

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				var wg sync.WaitGroup
				for j := 0; (i < b.N) && (j < 100000); j++ {
					wg.Add(1)
					go func() {
						defer wg.Done()
						db.Put(&benchsync.Item{
							Key:   key,
							Value: value,
						})
						allSize.Add(uint64(len(key) + len(value)))
					}()
					i++
				}
				wg.Wait()
			}

			b.StopTimer()

			var stats runtime.MemStats
			runtime.ReadMemStats(&stats)

			b.Logf(
				"b.N: %d, db: %s, size: %d, mem: %.2fMB\n",
				b.N, db.Stats(), allSize.Load()/1000/1000, float64(stats.Alloc)/1e6)
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
