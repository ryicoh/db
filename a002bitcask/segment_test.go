package a002bitcask

import (
	"bytes"
	"fmt"
	"os"
	"testing"
	"time"
)

func TestPut(t *testing.T) {
	dir := createTempDir(t)

	seg, err := NewSegment(dir, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer seg.Close()

	tests := []struct {
		key               []byte
		value             []byte
		expectedReplaced  bool
		expectedOffset    int64
		expectedHashMap   map[string]int64
		expectedFileBytes []byte
	}{
		{[]byte("mew"), []byte("1078"), false, 19, map[string]int64{"mew": 0}, []byte{
			0x03, 0x00, 0x00, 0x00, // key size = 3
			0x04, 0x00, 0x00, 0x00, // value size = 4
			0x00, 0x00, 0x00, 0x00, // tombstone = false
			0x6d, 0x65, 0x77, // key = "mew"
			0x31, 0x30, 0x37, 0x38, // value = "1078"
		}},
		{[]byte("purr"), []byte("2013"), false, 39, map[string]int64{"mew": 0, "purr": 19}, []byte{
			0x03, 0x00, 0x00, 0x00, // key size = 3
			0x04, 0x00, 0x00, 0x00, // value size = 4
			0x00, 0x00, 0x00, 0x00, // tombstone = false
			0x6d, 0x65, 0x77, // key = "mew"
			0x31, 0x30, 0x37, 0x38, // value = "1078"
			// --------------------------------
			0x04, 0x00, 0x00, 0x00, // key size = 4
			0x04, 0x00, 0x00, 0x00, // value size = 4
			0x00, 0x00, 0x00, 0x00, // tombstone = false
			0x70, 0x75, 0x72, 0x72, // key = "purr"
			0x32, 0x30, 0x31, 0x33, // value = "2013"
		}},
		{[]byte("purr"), []byte("2014"), true, 59, map[string]int64{"mew": 0, "purr": 39}, []byte{
			0x03, 0x00, 0x00, 0x00, // key size = 3
			0x04, 0x00, 0x00, 0x00, // value size = 4
			0x00, 0x00, 0x00, 0x00, // tombstone = false
			0x6d, 0x65, 0x77, // key = "mew"
			0x31, 0x30, 0x37, 0x38, // value = "1078"
			// --------------------------------
			0x04, 0x00, 0x00, 0x00, // key size = 4
			0x04, 0x00, 0x00, 0x00, // value size = 4
			0x00, 0x00, 0x00, 0x00, // tombstone = false
			0x70, 0x75, 0x72, 0x72, // key = "purr"
			0x32, 0x30, 0x31, 0x33, // value = "2013"
			// --------------------------------
			0x04, 0x00, 0x00, 0x00, // key size = 4
			0x04, 0x00, 0x00, 0x00, // value size = 4
			0x00, 0x00, 0x00, 0x00, // tombstone = false
			0x70, 0x75, 0x72, 0x72, // key = "purr"
			0x32, 0x30, 0x31, 0x34, // value = "2014"
		}},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("put %s", test.key), func(t *testing.T) {
			replaced, err := seg.Put(test.key, test.value)
			if err != nil {
				t.Fatal(err)
			}

			if replaced != test.expectedReplaced {
				t.Fatalf("expected replaced to be %t, got %t", test.expectedReplaced, replaced)
			}

			if seg.offset != test.expectedOffset {
				t.Fatalf("expected offset to be %d, got %d", test.expectedOffset, seg.offset)
			}

			checkHashMap(t, seg.hashMap, test.expectedHashMap)

			checkFile(t, seg.file, test.expectedFileBytes)
		})
	}
}

func TestDelete(t *testing.T) {
	dir := createTempDir(t)

	seg, err := NewSegment(dir, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer seg.Close()

	put(t, seg, []byte("mew"), []byte("1078"))
	put(t, seg, []byte("purr"), []byte("2013"))

	tests := []struct {
		key               []byte
		expectedOffset    int64
		expectedHashMap   map[string]int64
		expectedFileBytes []byte
	}{
		{[]byte("mew"), 54, map[string]int64{"purr": 19}, []byte{
			0x03, 0x00, 0x00, 0x00, // key size = 3
			0x04, 0x00, 0x00, 0x00, // value size = 4
			0x00, 0x00, 0x00, 0x00, // tombstone = false
			0x6d, 0x65, 0x77, // key = "mew"
			0x31, 0x30, 0x37, 0x38, // value = "1078"
			// --------------------------------
			0x04, 0x00, 0x00, 0x00, // key size = 4
			0x04, 0x00, 0x00, 0x00, // value size = 4
			0x00, 0x00, 0x00, 0x00, // tombstone = false
			0x70, 0x75, 0x72, 0x72, // key = "purr"
			0x32, 0x30, 0x31, 0x33, // value = "2013"
			// --------------------------------
			0x03, 0x00, 0x00, 0x00, // key size = 3
			0x00, 0x00, 0x00, 0x00, // value size = 0
			0x01, 0x00, 0x00, 0x00, // tombstone = true
			0x6d, 0x65, 0x77, // key = "mew"
		}},
		{[]byte("purr"), 70, map[string]int64{}, []byte{
			0x03, 0x00, 0x00, 0x00, // key size = 3
			0x04, 0x00, 0x00, 0x00, // value size = 4
			0x00, 0x00, 0x00, 0x00, // tombstone = false
			0x6d, 0x65, 0x77, // key = "mew"
			0x31, 0x30, 0x37, 0x38, // value = "1078"
			// --------------------------------
			0x04, 0x00, 0x00, 0x00, // key size = 4
			0x04, 0x00, 0x00, 0x00, // value size = 4
			0x00, 0x00, 0x00, 0x00, // tombstone = false
			0x70, 0x75, 0x72, 0x72, // key = "purr"
			0x32, 0x30, 0x31, 0x33, // value = "2013"
			// --------------------------------
			0x03, 0x00, 0x00, 0x00, // key size = 3
			0x00, 0x00, 0x00, 0x00, // value size = 0
			0x01, 0x00, 0x00, 0x00, // tombstone = true
			0x6d, 0x65, 0x77, // key = "mew"
			// --------------------------------
			0x04, 0x00, 0x00, 0x00, // key size = 4
			0x00, 0x00, 0x00, 0x00, // value size = 0
			0x01, 0x00, 0x00, 0x00, // tombstone = true
			0x70, 0x75, 0x72, 0x72, // key = "purr"
		}},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("delete %s", test.key), func(t *testing.T) {
			err := seg.Delete(test.key)
			if err != nil {
				t.Fatal(err)
			}

			if seg.offset != test.expectedOffset {
				t.Fatalf("expected offset to be %d, got %d", test.expectedOffset, seg.offset)
			}

			checkHashMap(t, seg.hashMap, test.expectedHashMap)

			checkFile(t, seg.file, test.expectedFileBytes)
		})
	}
}

func TestNewSegment(t *testing.T) {
	dir := createTempDir(t)

	// prepare a data file
	{
		file, err := openFile(dir, 1)
		if err != nil {
			t.Fatal(err)
		}
		defer file.Close()

		if _, err := file.Write([]byte{
			0x03, 0x00, 0x00, 0x00, // key size = 3
			0x04, 0x00, 0x00, 0x00, // value size = 4
			0x00, 0x00, 0x00, 0x00, // tombstone = false
			0x6d, 0x65, 0x77, // key = "mew"
			0x31, 0x30, 0x37, 0x38, // value = "1078"
			// --------------------------------
			0x04, 0x00, 0x00, 0x00, // key size = 4
			0x04, 0x00, 0x00, 0x00, // value size = 4
			0x00, 0x00, 0x00, 0x00, // tombstone = false
			0x70, 0x75, 0x72, 0x72, // key = "purr"
			0x32, 0x30, 0x31, 0x33, // value = "2013"
			// --------------------------------
			0x04, 0x00, 0x00, 0x00, // key size = 4
			0x04, 0x00, 0x00, 0x00, // value size = 4
			0x00, 0x00, 0x00, 0x00, // tombstone = false
			0x70, 0x75, 0x72, 0x72, // key = "purr"
			0x32, 0x30, 0x31, 0x34, // value = "2014"
			// --------------------------------
			0x03, 0x00, 0x00, 0x00, // key size = 3
			0x00, 0x00, 0x00, 0x00, // value size = 0
			0x01, 0x00, 0x00, 0x00, // tombstone = true
			0x6d, 0x65, 0x77, // key = "mew"
		}); err != nil {
			t.Fatal(err)
		}

		if err := file.Close(); err != nil {
			t.Fatal(err)
		}
	}

	seg, err := NewSegment(dir, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer seg.Close()

	checkHashMap(t, seg.hashMap, map[string]int64{"purr": 39})
}

func TestGet(t *testing.T) {
	dir := createTempDir(t)

	seg, err := NewSegment(dir, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer seg.Close()

	put(t, seg, []byte("mew"), []byte("1078"))
	put(t, seg, []byte("purr"), []byte("2013"))
	put(t, seg, []byte("purr"), []byte("2014"))
	del(t, seg, []byte("mew"))
	put(t, seg, []byte("scratch"), []byte("252"))

	tests := []struct {
		key              []byte
		expectedValue    []byte
		expectedNotFound bool
	}{
		{[]byte("mew"), []byte("1078"), true},
		{[]byte("purr"), []byte("2014"), false},
		{[]byte("scratch"), []byte("252"), false},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("get %s", test.key), func(t *testing.T) {
			value, err := seg.Get(test.key)
			if test.expectedNotFound {
				if err == nil || err != ErrKeyNotFound {
					t.Fatalf("expected error to be ErrKeyNotFound, got %v", err)
				}

				return
			}

			if err != nil {
				t.Fatal(err)
			}

			if !bytes.Equal(value, test.expectedValue) {
				t.Fatalf("expected value to be %v, got %v", test.expectedValue, value)
			}
		})
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

func checkHashMap(tb testing.TB, hashMap InMemoryMap[int64], expectedHashMap map[string]int64) {
	tb.Helper()

	keys := hashMap.GetAllKeys()
	if len(keys) != len(expectedHashMap) {
		tb.Fatalf("expected %d keys, got %d", len(expectedHashMap), len(keys))
	}

	for _, key := range keys {
		expectedValue, ok := expectedHashMap[string(key)]
		if !ok {
			tb.Fatalf("expected key %s to be in hashMap", key)
		}

		value, ok := hashMap.Get(key)
		if !ok {
			tb.Fatalf("expected key %s to be in hashMap", key)
		}

		if value != expectedValue {
			tb.Fatalf("expected value for key %s to be %d, got %d", key, expectedValue, value)
		}
	}
}

func checkFile(tb testing.TB, file file, expectedBytes []byte) {
	tb.Helper()

	actualBytes := make([]byte, len(expectedBytes))
	if _, err := file.ReadAt(actualBytes, 0); err != nil {
		tb.Fatal(err)
	}

	if !bytes.Equal(actualBytes, expectedBytes) {
		tb.Fatalf("expected bytes to be %v, got %v", expectedBytes, actualBytes)
	}
}

func put(tb testing.TB, seg *Segment, key, value []byte) {
	tb.Helper()

	if _, err := seg.Put(key, value); err != nil {
		tb.Fatal(err)
	}
}

func del(tb testing.TB, seg *Segment, key []byte) {
	tb.Helper()

	if err := seg.Delete(key); err != nil {
		tb.Fatal(err)
	}
}
