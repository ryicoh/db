package benchsync_test

import (
	benchsync "db/bench-sync"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"golang.org/x/exp/rand"
	"golang.org/x/sync/errgroup"
)

func init() {
	rand.Seed(uint64(time.Now().UnixNano()))
}

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

	var eg errgroup.Group

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := randomBytes(32)
		value := randomBytes(128)

		b.StartTimer()
		eg.Go(func() error {
			return db.Put(key, value)
		})
		b.StopTimer()
	}

	// FIXME: not exact
	{
		b.StartTimer()
		err := eg.Wait()
		b.StopTimer()

		if err != nil {
			b.Fatal(err)
		}
	}

	// 1s
	// 10550 ns/op
	// 94786 ops

	// 10s
	// 8053 ns/op
	// 124177 ops
}
