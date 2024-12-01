package benchsync_test

import (
	"bytes"
	benchsync "db/bench-sync"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"golang.org/x/sync/errgroup"
)

func TestDBGroupCommit7(t *testing.T) {
	numPairs := int(1000)
	pairs := make([]struct {
		key, value []byte
	}, numPairs)

	for i := 0; i < numPairs; i++ {
		pairs[i].key = []byte(fmt.Sprintf("key%03d", i))
		pairs[i].value = []byte(fmt.Sprintf("value%03d", i))
	}

	db := benchsync.NewDB8(filepath.Join(testDir, t.Name()), 1<<20)

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

func BenchmarkDBGroupCommit7(b *testing.B) {
	os.MkdirAll(filepath.Join(testDir, b.Name()), 0755)

	key := randomBytes(32)
	value := randomBytes(256)

	benchmarks := []struct {
		name          string
		maxBufferSize int
	}{
		{"1MB", 1 << 20},
		{"16MB", 16 << 20},
		{"64MB", 64 << 20},
		{"256MB", 256 << 20},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			testFile := filepath.Join(testDir, b.Name())
			db := benchsync.NewDB8(testFile, bm.maxBufferSize)

			var wg sync.WaitGroup
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					db.Put(key, value)
				}()
			}
			wg.Wait()

			b.StopTimer()
			db.PrintStats()
		})
	}

	// goos: linux
	// goarch: amd64
	// pkg: db/bench-sync
	// cpu: Intel(R) Core(TM) i5-14500
	// BenchmarkDBGroupCommit7-20    	  516972	      2110 ns/op	    2490 B/op	       3 allocs/op
}
