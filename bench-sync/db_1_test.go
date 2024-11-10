package benchsync_test

import (
	benchsync "db/bench-sync"
	"fmt"
	"path/filepath"
	"testing"
)

func BenchmarkDB1(b *testing.B) {
	db, err := benchsync.OpenDB1(filepath.Join(testDir, b.Name()))
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := randomBytes(32)
		value := randomBytes(128)

		b.StartTimer()
		{
			if err := db.Put(key, value); err != nil {
				b.Fatal(err)
			}
			if err := db.Sync(); err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()
	}

	// 1s
	// 897503 ns/op
	// 1114.2 OPS

	// 10s
	// 896903 ns/op
	// 1189.5 OPS
}

func BenchmarkDB1WithoutSync(b *testing.B) {
	db, err := benchsync.OpenDB1(filepath.Join(testDir, b.Name()))
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := randomBytes(32)
		value := randomBytes(128)
		if i%10000 == 0 {
			fmt.Printf("put %d / %d\n", i, b.N)
		}

		b.StartTimer()
		{
			if err := db.Put(key, value); err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()
	}

	// 1s
	// 832.4 ns/op

	// 10s
	// 863.2 ns/op
}
