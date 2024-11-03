package a002bitcask_test

import (
	"fmt"
	"os"
	"testing"
	"time"

	"db/a001simple"
	"db/a002bitcask"

	"golang.org/x/exp/rand"
)

func TestDB(t *testing.T) {
	dir := createTempDir(t)
	db := a002bitcask.NewDB(dir)

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
	dir := createTempDir(b)
	db := a002bitcask.NewDB(dir)

	b.ResetTimer()
	b.StopTimer()
	for i := 0; i < b.N; i++ {
		key := randomString(1, 32)
		value := randomString(32, 256)

		b.StartTimer()
		dbPut(b, db, key, value)
		b.StopTimer()
	}
}

func BenchmarkDBGet(b *testing.B) {
	dir := createTempDir(b)
	db := a002bitcask.NewDB(dir)

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

func dbPut(tb testing.TB, db *a002bitcask.DB, key, value string) {
	tb.Helper()
	err := db.Put([]byte(key), []byte(value))
	if err != nil {
		tb.Fatal(err)
	}
}

func dbGet(tb testing.TB, db *a002bitcask.DB, key string) (string, error) {
	tb.Helper()
	value, err := db.Get([]byte(key))
	if err != nil {
		return "", err
	}
	return string(value), nil
}

func dbDelete(tb testing.TB, db *a002bitcask.DB, key string) {
	tb.Helper()
	err := db.Delete([]byte(key))
	if err != nil {
		tb.Fatal(err)
	}
}

func createTempDir(tb testing.TB) string {
	tb.Helper()

	dir := fmt.Sprintf("tmp/%s_%s", tb.Name(), time.Now().Format(time.RFC3339))
	if err := os.Mkdir(dir, 0o700); err != nil {
		tb.Fatal(err)
	}
	return dir
}
