package benchsync_test

import (
	"bytes"
	benchsync "db/bench-sync"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"golang.org/x/sync/errgroup"
)

func TestDBGroupCommit8(t *testing.T) {
	testPath := filepath.Join(testDir, t.Name())
	os.RemoveAll(testPath)

	numPairs := int(3)
	pairs := make([]struct {
		key, value []byte
	}, numPairs)

	for i := 0; i < numPairs; i++ {
		pairs[i].key = []byte(fmt.Sprintf("key%03d", i))
		pairs[i].value = []byte(fmt.Sprintf("value%03d", i))
	}

	db, err := benchsync.NewDB9(benchsync.DB9Config{
		Path:          testPath,
		MaxBufferSize: 32,
		SyncInterval:  10 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}

	var eg errgroup.Group
	for _, pair := range pairs {
		eg.Go(func() error {
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

func BenchmarkDBGroupCommit8(b *testing.B) {
	os.MkdirAll(filepath.Join(testDir, b.Name()), 0755)

	key := randomBytes(32)
	value := randomBytes(256)

	benchmarks := []struct {
		name          string
		maxBufferSize int
		syncInterval  time.Duration
	}{
		{"4MB,10ms", 4 << 20, 10 * time.Millisecond},
		{"32MB,10ms", 32 << 20, 10 * time.Millisecond},
		{"256MB,10ms", 256 << 20, 10 * time.Millisecond},
		{"4GB,10ms", 4 << 30, 10 * time.Millisecond},
		{"4MB,100ms", 4 << 20, 100 * time.Millisecond},
		{"32MB,100ms", 32 << 20, 100 * time.Millisecond},
		{"256MB,100ms", 256 << 20, 100 * time.Millisecond},
		{"4GB,100ms", 4 << 30, 100 * time.Millisecond},
		{"4MB,1000ms", 4 << 20, 1000 * time.Millisecond},
		{"32MB,1000ms", 32 << 20, 1000 * time.Millisecond},
		{"256MB,1000ms", 256 << 20, 1000 * time.Millisecond},
		{"4GB,1000ms", 4 << 30, 1000 * time.Millisecond},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			testFile := filepath.Join(testDir, b.Name())
			db, err := benchsync.NewDB9(benchsync.DB9Config{
				Path:          testFile,
				MaxBufferSize: bm.maxBufferSize,
				SyncInterval:  bm.syncInterval,
			})
			if err != nil {
				b.Fatal(err)
			}

			var wg sync.WaitGroup
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					if err := db.Put(key, value); err != nil {
						panic(err)
					}
				}()
			}
			wg.Wait()

			// db.PrintStats()
		})
	}

	// goos: linux
	// goarch: amd64
	// pkg: db/bench-sync
	// cpu: Intel(R) Core(TM) i5-14500
	// BenchmarkDBGroupCommit7-20    	  516972	      2110 ns/op	    2490 B/op	       3 allocs/op
}
