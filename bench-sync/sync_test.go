package benchsync_test

import (
	"os"
	"testing"
)

func BenchmarkFileSync(b *testing.B) {
	dir := b.TempDir()
	file, err := os.CreateTemp(dir, "testfile")
	if err != nil {
		b.Fatal(err)
	}
	defer os.Remove(file.Name())

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := file.WriteString("test"); err != nil {
			b.Fatal(err)
		}

		b.StartTimer()
		err := file.Sync()
		b.StopTimer()

		if err != nil {
			b.Fatal(err)
		}
	}

	// 1s
	// 902697 ns/op
	// 1,108 OPS

	// 10s
	// 896903 ns/op
	// 1,115 OPS
}
