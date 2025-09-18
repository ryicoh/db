package db

import (
	"encoding/json"
	"os"
	"path"
	"reflect"
	"strconv"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/google/uuid"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
)

func newDBAndStore(t *testing.T) (*pebble.DB, *PebbleStore, func() error) {
	dir := path.Join(os.TempDir(), "raftdb_store_test", uuid.NewString())
	db, err := pebble.Open(dir, &pebble.Options{})
	store := &PebbleStore{db, hclog.Default()}
	if err != nil {
		t.Fatal(err)
	}

	return db, store, db.Close
}

func TestPebbleSet(t *testing.T) {
	db, store, finish := newDBAndStore(t)
	defer finish()

	var tests = []struct {
		name       string
		expected   error
		givenKey   string
		givenValue string
	}{
		{"キーと値を保存", nil, "samplekey", "samplevalue"},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			actual := store.Set([]byte(tt.givenKey), []byte(tt.givenValue))
			if actual != tt.expected {
				t.Errorf("(%s): expected %s, actual %s", tt.givenKey, tt.expected, actual)
			}

			value, closer, err := db.Get([]byte(tt.givenKey))
			if err != nil {
				t.Fatal(err)
			}
			defer closer.Close()

			if string(value) != tt.givenValue {
				t.Errorf("(%s): expected %s, actual %s", tt.givenKey, tt.givenValue, value)
			}
		})
	}
}

func TestPebbleGet(t *testing.T) {
	db, store, finish := newDBAndStore(t)
	defer finish()

	var tests = []struct {
		name          string
		preData       map[string]string
		expectedError error
		expectedValue []byte
		given         string
	}{
		{"キーが存在する場合は値を取得", map[string]string{
			"samplekey": "samplevalue",
		}, nil, []byte("samplevalue"), "samplekey"},
		{"キーが存在しない場合は値はnilが帰る", map[string]string{}, nil, nil, "not_samplekey"},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			for key, value := range tt.preData {
				err := db.Set([]byte(key), []byte(value), writeOptions)
				if err != nil {
					t.Fatal(err)
				}
			}

			actual, err := store.Get([]byte(tt.given))
			if err != tt.expectedError {
				t.Errorf("(%s): expected %s, actual %s", tt.given, tt.expectedError, err)
			}

			if !reflect.DeepEqual(actual, tt.expectedValue) {
				t.Errorf("(%s): expected %s, actual %s", tt.given, tt.expectedValue, actual)
			}
		})
	}
}

func TestPebbleSetUint64(t *testing.T) {
	db, store, finish := newDBAndStore(t)
	defer finish()

	var tests = []struct {
		name       string
		expected   error
		givenKey   string
		givenValue uint64
	}{
		{"キーと値を保存", nil, "samplekey", 123},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			actual := store.SetUint64([]byte(tt.givenKey), tt.givenValue)
			if actual != tt.expected {
				t.Errorf("(%s): expected %s, actual %s", tt.givenKey, tt.expected, actual)
			}

			value, closer, err := db.Get([]byte(tt.givenKey))
			if err != nil {
				t.Fatal(err)
			}
			defer closer.Close()
			if !reflect.DeepEqual(value, []byte(strconv.FormatUint(tt.givenValue, 10))) {
				t.Errorf("(%s): expected %d, actual %d", []byte(strconv.FormatUint(tt.givenValue, 10)), tt.givenValue, value)
			}
		})
	}
}

func TestPebbleGetUint64(t *testing.T) {
	db, store, finish := newDBAndStore(t)
	defer finish()

	var tests = []struct {
		name          string
		preData       map[string]string
		expectedError error
		expectedValue uint64
		given         string
	}{
		{"キーが存在する場合は値を取得", map[string]string{
			"samplekey": "1",
		}, nil, 1, "samplekey"},
		{"キーが存在しない場合は値は0が帰る", map[string]string{}, nil, 0, "not_samplekey"},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			for key, value := range tt.preData {
				err := db.Set([]byte(key), []byte(value), writeOptions)
				if err != nil {
					t.Fatal(err)
				}
			}

			actual, err := store.GetUint64([]byte(tt.given))
			if err != tt.expectedError {
				t.Errorf("(%s): expected %s, actual %s", tt.given, tt.expectedError, err)
			}

			if !reflect.DeepEqual(actual, tt.expectedValue) {
				t.Errorf("(%s): expected %d, actual %d", tt.given, tt.expectedValue, actual)
			}
		})
	}
}

func TestFirstIndex(t *testing.T) {
	db, store, finish := newDBAndStore(t)
	defer finish()

	var tests = []struct {
		name          string
		preData       map[string]string
		expectedError error
		expectedValue uint64
	}{
		{"最初のインデックスを取得", map[string]string{
			"1": "data1",
			"2": "data2",
		}, nil, 1},
		{"最初のインデックスを取得", map[string]string{
			"3": "data3",
		}, nil, 1},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			for key, value := range tt.preData {
				err := db.Set([]byte(key), []byte(value), writeOptions)
				if err != nil {
					t.Fatal(err)
				}
			}

			actual, err := store.FirstIndex()
			if err != tt.expectedError {
				t.Errorf("expected %s, actual %s", tt.expectedError, err)
			}

			if !reflect.DeepEqual(actual, tt.expectedValue) {
				t.Errorf("expected %d, actual %d", tt.expectedValue, actual)
			}
		})
	}
}

func TestLastIndex(t *testing.T) {
	db, store, finish := newDBAndStore(t)
	defer finish()

	var tests = []struct {
		name          string
		preData       map[string]string
		expectedError error
		expectedValue uint64
	}{
		{"最後のインデックスを取得", map[string]string{
			"1": "data1",
			"2": "data2",
		}, nil, 2},
		{"最後のインデックスを取得", map[string]string{
			"3": "data3",
		}, nil, 3},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			for key, value := range tt.preData {
				err := db.Set([]byte(key), []byte(value), writeOptions)
				if err != nil {
					t.Fatal(err)
				}
			}

			actual, err := store.LastIndex()
			if err != tt.expectedError {
				t.Errorf("expected %v, actual %v", tt.expectedError, err)
			}

			if !reflect.DeepEqual(actual, tt.expectedValue) {
				t.Errorf("expected %v, actual %v", tt.expectedValue, actual)
			}
		})
	}
}

func TestGetLog(t *testing.T) {
	db, store, finish := newDBAndStore(t)
	defer finish()

	var tests = []struct {
		name          string
		preData       map[string]raft.Log
		expectedError error
		expectedValue *raft.Log
		given         uint64
	}{
		{"ログを取得", map[string]raft.Log{
			"123": {Type: raft.LogCommand, Index: 123, Term: 456, Data: []byte("value")},
		}, nil, &raft.Log{Type: raft.LogCommand, Index: 123, Term: 456, Data: []byte("value")}, 123},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			for key, value := range tt.preData {
				jsonLog, err := json.Marshal(value)
				if err != nil {
					t.Fatal(err)
				}
				err = db.Set([]byte(key), jsonLog, writeOptions)
				if err != nil {
					t.Fatal(err)
				}
			}

			actual := new(raft.Log)
			err := store.GetLog(tt.given, actual)
			if err != tt.expectedError {
				t.Errorf("expected %v, actual %v", tt.expectedError, err)
			}

			if !reflect.DeepEqual(actual, tt.expectedValue) {
				t.Errorf("expected %v, actual %v", tt.expectedValue, actual)
			}
		})
	}
}

func TestStoreLog(t *testing.T) {
	db, store, finish := newDBAndStore(t)
	defer finish()

	var tests = []struct {
		name     string
		expected error
		given    *raft.Log
	}{
		{"ログを保存", nil, &raft.Log{Type: raft.LogCommand, Index: 123, Term: 456, Data: []byte("value")}},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			err := store.StoreLog(tt.given)
			if err != tt.expected {
				t.Errorf("expected %v, actual %v", tt.expected, err)
			}

			value, closer, err := db.Get([]byte(strconv.FormatUint(tt.given.Index, 10)))
			if err != nil {
				t.Fatal(err)
			}
			defer closer.Close()

			actual := new(raft.Log)
			if err := json.Unmarshal(value, actual); err != nil {
				t.Fatal(err)
			}

			if !reflect.DeepEqual(actual, tt.given) {
				t.Errorf("expected %v, actual %v", tt.given, actual)
			}
		})
	}
}

func TestStoreLogs(t *testing.T) {
	db, store, finish := newDBAndStore(t)
	defer finish()

	var tests = []struct {
		name     string
		expected error
		given    []*raft.Log
	}{
		{"ログを保存", nil, []*raft.Log{
			{Type: raft.LogCommand, Index: 123, Term: 456, Data: []byte("value")},
			{Type: raft.LogCommand, Index: 124, Term: 457, Data: []byte("value2")},
		}},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			err := store.StoreLogs(tt.given)
			if err != tt.expected {
				t.Errorf("expected %v, actual %v", tt.expected, err)
			}

			for _, given := range tt.given {
				value, closer, err := db.Get([]byte(strconv.FormatUint(given.Index, 10)))
				if err != nil {
					if err != pebble.ErrNotFound {
						t.Fatal(err)
					}
				}
				defer closer.Close()

				actual := new(raft.Log)
				if err := json.Unmarshal(value, actual); err != nil {
					t.Fatal(err)
				}

				if !reflect.DeepEqual(actual, given) {
					t.Errorf("expected %v, actual %v", given, actual)
				}
			}
		})
	}
}

func TestDeleteRange(t *testing.T) {
	_, store, finish := newDBAndStore(t)
	defer finish()

	var tests = []struct {
		name          string
		preData       []*raft.Log
		remainingData []*raft.Log
		deletedData   []*raft.Log
		expected      error
		givenMin      uint64
		givenMax      uint64
	}{
		{"ログを削除", []*raft.Log{
			{Type: raft.LogCommand, Index: 123, Term: 456, Data: []byte("value1")},
			{Type: raft.LogCommand, Index: 124, Term: 457, Data: []byte("value2")},
			{Type: raft.LogCommand, Index: 125, Term: 458, Data: []byte("value3")},
			{Type: raft.LogCommand, Index: 126, Term: 459, Data: []byte("value4")},
			{Type: raft.LogCommand, Index: 127, Term: 460, Data: []byte("value5")},
		},
			[]*raft.Log{
				{Type: raft.LogCommand, Index: 123, Term: 456, Data: []byte("value1")},
				{Type: raft.LogCommand, Index: 127, Term: 460, Data: []byte("value5")},
			},
			[]*raft.Log{
				{Type: raft.LogCommand, Index: 124, Term: 457, Data: []byte("value2")},
				{Type: raft.LogCommand, Index: 125, Term: 458, Data: []byte("value3")},
				{Type: raft.LogCommand, Index: 126, Term: 459, Data: []byte("value4")},
			},
			nil, 124, 126},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			if err := store.StoreLogs(tt.preData); err != nil {
				t.Fatal(err)
			}

			err := store.DeleteRange(tt.givenMin, tt.givenMax)
			if err != tt.expected {
				t.Errorf("expected %v, actual %v", tt.expected, err)
			}

			for _, given := range tt.remainingData {
				actual := new(raft.Log)
				err := store.GetLog(given.Index, actual)
				if err != nil {
					t.Fatal(err)
				}

				if !reflect.DeepEqual(actual, given) {
					t.Errorf("expected %v, actual %v", given, actual)
				}
			}

			for _, given := range tt.deletedData {
				actual := new(raft.Log)
				err := store.GetLog(given.Index, actual)
				if err != raft.ErrLogNotFound {
					t.Errorf("expected %v, actual %v", raft.ErrLogNotFound, err)
				}
			}
		})
	}
}
