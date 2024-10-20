package a001simple_test

import (
	"os"
	"testing"

	"db/a001simple"
)

func TestDB(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "db_test")
	if err != nil {
		t.Fatal(err)
	}
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
}

func dbPut(t *testing.T, db *a001simple.DB, key, value string) {
	t.Helper()
	err := db.Put([]byte(key), []byte(value))
	if err != nil {
		t.Fatal(err)
	}
}

func dbGet(t *testing.T, db *a001simple.DB, key string) (string, error) {
	t.Helper()
	value, err := db.Get([]byte(key))
	if err != nil {
		return "", err
	}
	return string(value), nil
}
