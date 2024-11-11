package benchsync_test

import (
	benchsync "db/bench-sync"
	"fmt"
	"path/filepath"
	"testing"

	"golang.org/x/sync/errgroup"
)

func TestDBGroupCommit2(t *testing.T) {
	db, err := benchsync.OpenDBGroupCommit2(filepath.Join(testDir, t.Name()))
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

func BenchmarkDBGroupCommit2(b *testing.B) {
	db, err := benchsync.OpenDBGroupCommit2(filepath.Join(testDir, b.Name()))
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	var eg errgroup.Group

	pairs := make([]struct {
		key, value []byte
	}, b.N)
	for i := 0; i < b.N; i++ {
		pairs[i].key = randomBytes(32)
		pairs[i].value = randomBytes(128)
	}

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
	// 3641 ns/op
	// 274,000 ops

	// 10s
	// 2932 ns/op
	// 341,000 ops
}
