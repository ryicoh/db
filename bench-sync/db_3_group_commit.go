package benchsync

import (
	"encoding/binary"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ncw/directio"
)

type dbGroupCommit struct {
	f                  *os.File
	offset             int64
	currentBlock       []byte
	currentBlockOffset uint32
	sync               func(chan error)

	putMutex sync.Mutex
}

var _ DB = &dbGroupCommit{}

func OpenDBGroupCommit(fileName string) (*dbGroupCommit, error) {
	f, err := directio.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	sm := &syncManager{f: f}

	return &dbGroupCommit{
		f:                  f,
		offset:             0,
		currentBlock:       newBlock(),
		currentBlockOffset: 0,
		sync:               sm.Sync,
	}, nil
}

const maxSyncChannels = 1024
const syncInterval = 32 * time.Millisecond

type syncManager struct {
	f             *os.File
	channels      []chan error
	mux           sync.Mutex
	currentSyncId int64
}

func (sm *syncManager) Sync(ch chan error) {
	var first bool
	sm.mux.Lock()
	{
		first = len(sm.channels) == 0
		sm.channels = append(sm.channels, ch)
		if len(sm.channels) >= maxSyncChannels {
			slog.Debug("trigger sync because exceeded `maxSyncChannels`", "channels", len(sm.channels))
			// FIXME: should not unlock here???
			sm.mux.Unlock()

			atomic.AddInt64(&sm.currentSyncId, 1)
			sm.sync()
			return
		}
	}
	sm.mux.Unlock()

	if first {
		startedId := atomic.AddInt64(&sm.currentSyncId, 1)
		go func() {
			time.Sleep(syncInterval)

			waitedId := atomic.LoadInt64(&sm.currentSyncId)
			if waitedId != startedId {
				slog.Debug("skip sync because `waitedId` is not equal to `startedId`", "waitedId", waitedId, "startedId", startedId)
				return
			}

			slog.Debug("trigger sync because exceeded `syncInterval`", "channels", len(sm.channels))
			sm.sync()
		}()
	}

}

var syncCount int64

func (sm *syncManager) sync() {
	// Copy channels and clear.
	sm.mux.Lock()
	if len(sm.channels) == 0 {
		sm.mux.Unlock()
		return
	}

	channels := make([]chan error, len(sm.channels))
	copy(channels, sm.channels)

	sm.channels = make([]chan error, 0, len(channels))
	sm.mux.Unlock()

	err := sm.f.Sync()
	atomic.AddInt64(&syncCount, 1)
	slog.Debug("synced", "count", syncCount)

	// Notify error to all channels
	for _, ch := range channels {
		var e error
		if err != nil {
			e = fmt.Errorf("debounce sync failed: %w", err)
		}
		ch <- e
	}
}

func (db *dbGroupCommit) rotateBlock() {
	db.currentBlock = newBlock()
	db.currentBlockOffset = 0
	db.offset += blockSize
}

func (db *dbGroupCommit) Put(key, value []byte) error {
	if err := db.putConcurrently(key, value); err != nil {
		return fmt.Errorf("failed to put: %w", err)
	}

	errCh := make(chan error)
	go db.sync(errCh)
	if err := <-errCh; err != nil {
		return fmt.Errorf("failed to sync: %w", err)
	}

	return nil
}

func (db *dbGroupCommit) putConcurrently(key, value []byte) error {
	db.putMutex.Lock()
	defer db.putMutex.Unlock()

	keyLen := uint32(len(key))
	recordSize := uint32(keyLenSize + len(key) + valueLenSize + len(value))

	if db.currentBlockOffset+recordSize > blockSize {
		db.rotateBlock()
	}

	{
		binary.BigEndian.PutUint32(db.currentBlock[db.currentBlockOffset:], keyLen)
		copy(db.currentBlock[db.currentBlockOffset+keyLenSize:], key)
	}
	{
		valueOffset := keyLenSize + keyLen
		binary.BigEndian.PutUint32(db.currentBlock[db.currentBlockOffset+valueOffset:], uint32(len(value)))
		copy(db.currentBlock[db.currentBlockOffset+valueOffset+valueLenSize:], value)
	}
	db.currentBlockOffset += recordSize

	_, err := db.f.WriteAt(db.currentBlock, db.offset)
	if err != nil {
		return err
	}
	return nil
}

func (db *dbGroupCommit) Get(key []byte) ([]byte, error) {
	panic("not implemented")
}

func (db *dbGroupCommit) Close() error {
	return db.f.Close()
}
