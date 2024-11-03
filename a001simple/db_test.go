package a001simple_test

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"db/a001simple"

	"golang.org/x/exp/rand"
)

func TestDB(t *testing.T) {
	f := createTempFile(t)
	defer f.Close()

	db := a001simple.NewDB(f)

	// put "key1", then get it
	dbPut(t, db, "key1", "value1")
	value1, err := dbGet(t, db, "key1")
	if err != nil {
		t.Fatal(err)
	}
	if value1 != "value1" {
		t.Fatalf("value mismatch: %s != %s", value1, "value1")
	}

	// get "key2", which is not put yet, should return ErrNotFound
	_, err = dbGet(t, db, "key2")
	if err != a001simple.ErrNotFound {
		t.Fatal(err)
	}

	// put "key2", then get it
	dbPut(t, db, "key2", "value2")
	value2, err := dbGet(t, db, "key2")
	if err != nil {
		t.Fatal(err)
	}
	if value2 != "value2" {
		t.Fatalf("value mismatch: %s != %s", value2, "value2")
	}

	// put "key1" again, then it should overwrite the previous value
	dbPut(t, db, "key1", "new value")
	value1, err = dbGet(t, db, "key1")
	if err != nil {
		t.Fatal(err)
	}
	if value1 != "new value" {
		t.Fatalf("value mismatch: %s != %s", value1, "new value")
	}

	// delete "key1"
	dbDelete(t, db, "key1")
	_, err = dbGet(t, db, "key1")
	if err != a001simple.ErrNotFound {
		t.Fatal(err)
	}
}

func BenchmarkDBPut(b *testing.B) {
	f := createTempFile(b)
	defer f.Close()

	db := a001simple.NewDB(f)

	b.ResetTimer()
	b.StopTimer()
	for i := 0; i < b.N; i++ {
		key := randomString(1, 32)
		value := randomString(32, 256)

		b.StartTimer()
		dbPut(b, db, key, value)
		b.StopTimer()
	}

	// 1000000x
	// 2069 ns/op
	// 約483,333 ops
}

func BenchmarkDBGet(b *testing.B) {
	f := createTempFile(b)
	defer f.Close()

	db := a001simple.NewDB(f)

	keys := make([]string, b.N)
	data := make(map[string]string)
	for i := 0; i < b.N; i++ {
		key := randomString(1, 32)
		value := randomString(32, 256)
		data[key] = value
		keys[i] = key
		dbPut(b, db, key, value)
	}

	b.ResetTimer()
	b.StopTimer()
	for i := 0; i < b.N; i++ {

		b.StartTimer()
		v, err := dbGet(b, db, keys[i])
		b.StopTimer()

		if err != nil {
			b.Fatal(err)
		}
		if v != data[keys[i]] {
			b.Fatalf("value mismatch: %s != %s", v, data[keys[i]])
		}
	}

	// 10x
	// 36044 ns/op
	// 909246 ns/op

	// 100x
	// 196508 ns/op
	// 5,088 ops

	// 1000x
	// 909246 ns/op
	// 1,099 ops

	// 10000x
	// 6047591 ns/op
	// 165.35 ops
}

const chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

func randomString(minLen, maxLen int) string {
	length := rand.Intn(maxLen-minLen) + minLen
	buf := make([]byte, length)
	for i := range buf {
		buf[i] = chars[rand.Intn(len(chars))]
	}
	return string(buf)
}

func dbPut(tb testing.TB, db *a001simple.DB, key, value string) {
	tb.Helper()
	err := db.Put([]byte(key), []byte(value))
	if err != nil {
		tb.Fatal(err)
	}
}

func dbGet(tb testing.TB, db *a001simple.DB, key string) (string, error) {
	tb.Helper()
	value, err := db.Get([]byte(key))
	if err != nil {
		return "", err
	}
	return string(value), nil
}

func dbDelete(tb testing.TB, db *a001simple.DB, key string) {
	tb.Helper()
	err := db.Delete([]byte(key))
	if err != nil {
		tb.Fatal(err)
	}
}

func createTempFile(tb testing.TB) *os.File {
	tb.Helper()

	dir := "tmp"
	f, err := os.Create(
		filepath.Join(dir,
			fmt.Sprintf("%s_%s.db",
				tb.Name(),
				time.Now().Format(time.RFC3339),
			),
		),
	)
	if err != nil {
		tb.Fatal(err)
	}
	return f
}
