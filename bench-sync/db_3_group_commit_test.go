package benchsync_test

import (
	benchsync "db/bench-sync"
	"fmt"
	"path/filepath"
	"testing"

	"golang.org/x/sync/errgroup"
)

func TestDBGroupCommit(t *testing.T) {
	db, err := benchsync.OpenDBGroupCommit(filepath.Join(testDir, t.Name()))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	var eg errgroup.Group
	for i := 0; i < 187; i++ {
		key := fmt.Sprintf("key%03d", i)
		value := fmt.Sprintf("value%03d", i)
		eg.Go(func() error {
			return db.Put([]byte(key), []byte(value))
		})
	}

	if err := eg.Wait(); err != nil {
		t.Fatal(err)
	}
}

func BenchmarkDBGroupCommit(b *testing.B) {
	db, err := benchsync.OpenDBGroupCommit(filepath.Join(testDir, b.Name()))
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
	for _, pair := range pairs {
		eg.Go(func() error {
			return db.Put(pair.key, pair.value)
		})
	}

	if err := eg.Wait(); err != nil {
		b.Fatal(err)
	}

	// 1s
	// 14412 ns/op
	// 70000 ops

	// 10s
	// 14658 ns/op
	// 68000 ops
}
