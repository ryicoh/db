package benchstorage_test

import (
	"crypto/rand"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ncw/directio"
)

// os.Getpagesize()
const pageSize = 4096

var storagePath = os.Getenv("STORAGE_PATH")

func BenchmarkWriteSeq(b *testing.B) {
	f := setup(b)
	defer f.Close()

	// 1GB
	numPages := 1 * 1024 * 1024 * 1024 / pageSize
	pageData := newRandomDataPage()

	var prev time.Duration

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StartTimer()
		for j := 0; j < numPages; j++ {
			if _, err := f.Write(pageData); err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()

		var elapsed time.Duration
		{
			e := b.Elapsed()
			elapsed = e - prev
			prev = e
		}

		fmt.Printf("%d: %dms, %dops, %dMB/s\n",
			i+1,
			elapsed.Milliseconds(),
			int64(float64(numPages)/elapsed.Seconds()),
			int64(float64(pageSize*numPages)/(1024*1024*elapsed.Seconds())))
	}
}

func BenchmarkReadSeq(b *testing.B) {
	f := setup(b)
	defer f.Close()

	// 1GB
	numPages := 1 * 1000 * 1000 * 1024 / pageSize
	pageData := newRandomDataPage()

	// write
	for i := 0; i < b.N; i++ {
		for j := 0; j < numPages; j++ {
			if _, err := f.Write(pageData[:]); err != nil {
				b.Fatal(err)
			}
		}
	}

	tm := time.Now()
	b.ResetTimer()
	// read
	for i := 0; i < b.N; i++ {
		offset1 := int64(i * pageSize * numPages)

		b.StartTimer()
		for j := 0; j < numPages; j++ {
			offset2 := int64(j * pageSize)
			buf := make([]byte, pageSize)
			if _, err := f.ReadAt(buf, offset1+offset2); err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()

		duration := time.Since(tm)
		slog.Info(fmt.Sprintf("%d: %dms %dMB/s",
			i+1,
			duration.Milliseconds(),
			int64(float64(pageSize*numPages)/(1024*1024*duration.Seconds()))))
		tm = time.Now()
	}

}

func setup(b *testing.B) *os.File {
	b.Helper()

	if storagePath == "" {
		b.Skip("STORAGE_PATH is not set")
	}

	fileName := filepath.Join(storagePath, fmt.Sprintf("bench_%s", time.Now().Format("20240101_123456")))

	f, err := directio.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		b.Fatal(err)
	}

	return f
}

func newRandomDataPage() []byte {
	page := directio.AlignedBlock(pageSize)
	if _, err := rand.Read(page); err != nil {
		panic(err)
	}
	return page
}
