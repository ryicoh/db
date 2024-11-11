package benchsync_test

import (
	benchsync "db/bench-sync"
	"fmt"
	"path/filepath"
	"testing"

	"golang.org/x/sync/errgroup"
)

func TestDBODirect(t *testing.T) {
	db, err := benchsync.OpenDBODirect(filepath.Join(testDir, t.Name()))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	for i := 0; i < 187; i++ {
		key := fmt.Sprintf("key%03d", i)
		value := fmt.Sprintf("value%03d", i)
		if err := db.Put([]byte(key), []byte(value)); err != nil {
			t.Fatal(err)
		}
	}
}

func BenchmarkDBODirect(b *testing.B) {
	db, err := benchsync.OpenDBODirect(filepath.Join(testDir, b.Name()))
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	pairs := make([]struct {
		key, value []byte
	}, b.N)
	for i := 0; i < b.N; i++ {
		pairs[i].key = randomBytes(32)
		pairs[i].value = randomBytes(128)
	}

	var eg errgroup.Group
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		eg.Go(func() error {
			return db.Put(pairs[i].key, pairs[i].value)
		})
	}

	if err := eg.Wait(); err != nil {
		b.Fatal(err)
	}

	// 1s
	// 790924 ns/op
	// 1264.34 ops

	// 10s
	// 836889 ns/op
	// 1194.90 ops
}
