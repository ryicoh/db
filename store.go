package db

import (
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/cockroachdb/pebble"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
)

// ストレージエンジンには、pebbleを利用
type PebbleStore struct {
	db     *pebble.DB
	logger hclog.Logger
}

// データ保存用の独自のストア
type DataStore interface {
	Get(key []byte) ([]byte, error)
	Set(key []byte, val []byte) error
	Delete(key []byte) error
}

// PebbleStoreが以下のインターフェイスを実装していることを保証する
// * raft.LogStore
// * raft.StableStore
// * DataStore
var (
	_ raft.LogStore    = &PebbleStore{}
	_ raft.StableStore = &PebbleStore{}
	_ DataStore        = &PebbleStore{}
)

var (
	writeOptions = &pebble.WriteOptions{Sync: true}
)

func NewPebbleStore(name string, logger hclog.Logger) (*PebbleStore, error) {
	db, err := pebble.Open(name, nil)
	if err != nil {
		return nil, err
	}

	return &PebbleStore{db, logger}, nil
}

// Set, Get, GetUint64, SetUint64 は、raft.StableStore を実装
func (r *PebbleStore) Set(key []byte, val []byte) error {
	r.logger.Debug("Set", "key", string(key), "val", string(val))

	return r.db.Set(key, val, writeOptions)
}

func (r *PebbleStore) Get(key []byte) (val []byte, err error) {
	defer func() {
		r.logger.Debug("Get", "key", string(key), "val", string(val))
	}()

	slice, closer, err := r.db.Get(key)
	if err != nil {
		if err == pebble.ErrNotFound {
			return nil, nil
		}
		return nil, err
	}
	defer closer.Close()

	if slice == nil {
		return nil, err
	}

	val = make([]byte, len(slice))
	copy(val, slice)
	return
}

func (r *PebbleStore) Delete(key []byte) error {
	r.logger.Debug("Delete", "key", string(key))

	return r.db.Delete(key, writeOptions)
}

func (r *PebbleStore) SetUint64(key []byte, val uint64) error {
	r.logger.Debug("SetUint64", "key", string(key), "val", val)

	return r.Set(key, []byte(strconv.FormatUint(val, 10)))
}

func (r *PebbleStore) GetUint64(key []byte) (u64 uint64, err error) {
	defer func() {
		r.logger.Debug("GetUint64", "key", string(key), "val", u64)
	}()

	val, err := r.Get(key)
	if err != nil {
		if err == pebble.ErrNotFound {
			return 0, nil
		}
		return 0, err
	}

	if val == nil {
		return 0, nil
	}

	u64, err = strconv.ParseUint(string(val), 10, 0)
	return
}

// FirstIndex, LastIndex, GetLog, StoreLog, StoreLogs, DeleteRange は、raft.LogStore を実装
func (r *PebbleStore) FirstIndex() (index uint64, err error) {
	defer func() {
		r.logger.Debug("FirstIndex", "firstIndex", fmt.Sprint(index))
	}()

	it, err := r.db.NewIter(&pebble.IterOptions{})
	if err != nil {
		return 0, err
	}
	defer it.Close()
	if it.First(); it.Valid() {
		key := it.Key()
		u64, err := strconv.ParseUint(string(key), 10, 0)
		if err != nil {
			return 0, err
		}
		index = u64
	}
	return
}

func (r *PebbleStore) LastIndex() (index uint64, err error) {
	defer func() {
		r.logger.Debug("LastIndex", "lastIndex", fmt.Sprint(index))
	}()

	it, err := r.db.NewIter(&pebble.IterOptions{})
	if err != nil {
		return 0, err
	}
	defer it.Close()
	if it.Last(); it.Valid() {
		key := it.Key()
		u64, err := strconv.ParseUint(string(key), 10, 0)
		if err != nil {
			return 0, err
		}
		index = u64
	}
	return
}

func (r *PebbleStore) GetLog(index uint64, log *raft.Log) (err error) {
	var val []byte
	defer func() {
		r.logger.Debug("GetLog", "index", fmt.Sprint(index), "log", string(val))
	}()

	val, err = r.Get([]byte(strconv.FormatUint(index, 10)))
	if err != nil {
		if err == pebble.ErrNotFound {
			return raft.ErrLogNotFound
		}
		return err
	}
	if val == nil {
		return raft.ErrLogNotFound
	}

	return json.Unmarshal(val, log)
}

func (r *PebbleStore) StoreLog(log *raft.Log) (err error) {
	var val []byte
	defer func() {
		r.logger.Debug("StoreLog", "log", string(val))
	}()

	val, err = json.Marshal(log)
	if err != nil {
		return err
	}

	return r.Set([]byte(strconv.FormatUint(log.Index, 10)), val)
}

func (r *PebbleStore) StoreLogs(logs []*raft.Log) (err error) {
	var debugLogs []interface{}
	for i, log := range logs {
		val, err := json.Marshal(log)
		if err != nil {
			return err
		}
		debugLogs = append(debugLogs, fmt.Sprintf("logs[%d]", i))
		debugLogs = append(debugLogs, string(val))
	}
	r.logger.Debug("StoreLog", debugLogs...)

	batch := r.db.NewBatchWithSize(len(logs))
	for _, log := range logs {
		val, err := json.Marshal(log)
		if err != nil {
			return err
		}

		if err := batch.Set([]byte(strconv.FormatUint(log.Index, 10)), val, writeOptions); err != nil {
			return err
		}
	}

	return batch.Commit(writeOptions)
}

func (r *PebbleStore) DeleteRange(min uint64, max uint64) error {
	r.logger.Debug("DeleteRange", "min", min, "max", max)

	batch := r.db.NewBatchWithSize(0)

	it, err := r.db.NewIter(&pebble.IterOptions{})
	if err != nil {
		return err
	}

	start := []byte(strconv.FormatUint(min, 10))
	for it.SeekGE(start); it.Valid(); it.Next() {
		key := it.Key()
		u64Key, err := strconv.ParseUint(string(key), 10, 0)
		if err != nil {
			return err
		}

		if u64Key > max {
			break
		}

		if err := batch.Delete(key, writeOptions); err != nil {
			return err
		}
	}
	it.Close()

	return batch.Commit(writeOptions)
}
