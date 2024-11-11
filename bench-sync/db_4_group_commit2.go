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

type dbGroupCommit2 struct {
	f                  *os.File
	offset             int64
	currentBlock       []byte
	currentBlockOffset uint32
	writeSync          func(offset int64, block []byte, ch chan error)

	putMutex sync.Mutex
}

var _ DB = &dbGroupCommit2{}

func OpenDBGroupCommit2(fileName string) (*dbGroupCommit2, error) {
	f, err := directio.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	sm := &writeSyncManager{f: f, requests: make(map[int64]writeRequest)}

	return &dbGroupCommit2{
		f:                  f,
		offset:             0,
		currentBlock:       newBlock(),
		currentBlockOffset: 0,
		writeSync:          sm.WriteSync,
	}, nil
}

type writeRequest struct {
	offset int64
	block  []byte
}

type writeSyncManager struct {
	f *os.File

	// map[offset]writeRequest
	requests      map[int64]writeRequest
	channels      []chan error
	mux           sync.Mutex
	currentSyncId int64
}

func (sm *writeSyncManager) WriteSync(offset int64, block []byte, ch chan error) {
	var first bool
	sm.mux.Lock()
	{
		first = len(sm.channels) == 0
		sm.requests[offset] = writeRequest{offset, block}
		sm.channels = append(sm.channels, ch)

		if len(sm.channels) >= maxSyncChannels {
			slog.Debug("trigger sync because exceeded `maxSyncChannels`", "channels", len(sm.channels))
			// FIXME: should not unlock here???
			sm.mux.Unlock()

			atomic.AddInt64(&sm.currentSyncId, 1)
			sm.writeSync()
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
			sm.writeSync()
		}()
	}

}

func (sm *writeSyncManager) writeSync() {
	// Copy channels and clear.
	sm.mux.Lock()
	if len(sm.channels) == 0 {
		sm.mux.Unlock()
		return
	}

	requestsLen := len(sm.requests)
	requests := make([]writeRequest, requestsLen)
	i := 0
	for _, req := range sm.requests {
		requests[i] = req
		i++
	}

	channelsLen := len(sm.channels)
	channels := make([]chan error, channelsLen)
	copy(channels, sm.channels)

	sm.requests = make(map[int64]writeRequest)
	sm.channels = make([]chan error, 0, channelsLen)

	sm.mux.Unlock()

	var err error
	for _, req := range requests {
		// slog.Debug("write", "offset", req.offset, "size", len(req.block))
		if _, err = sm.f.WriteAt(req.block, req.offset); err != nil {
			break
		}
	}

	if err != nil {
		goto notifyError
	}

	err = sm.f.Sync()

	atomic.AddInt64(&syncCount, 1)
	slog.Debug("synced", "count", syncCount)

notifyError:
	// Notify error to all channels
	for _, ch := range channels {
		var e error
		if err != nil {
			e = fmt.Errorf("debounce sync failed: %w", err)
		}
		ch <- e
	}
}

func (db *dbGroupCommit2) rotateBlock() {
	db.currentBlock = newBlock()
	db.currentBlockOffset = 0
	db.offset += blockSize
}

func (db *dbGroupCommit2) Put(key, value []byte) error {
	db.putMutex.Lock()
	if err := db.putConcurrently(key, value); err != nil {
		db.putMutex.Unlock()
		return fmt.Errorf("failed to put: %w", err)
	}

	block := make([]byte, blockSize) // FIXME
	copy(block, db.currentBlock)
	db.putMutex.Unlock()

	errCh := make(chan error)
	go db.writeSync(db.offset, block, errCh)
	if err := <-errCh; err != nil {
		return fmt.Errorf("failed to sync: %w", err)
	}

	return nil
}

func (db *dbGroupCommit2) putConcurrently(key, value []byte) error {
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

	return nil
}

func (db *dbGroupCommit2) Get(key []byte) ([]byte, error) {
	panic("not implemented")
}

func (db *dbGroupCommit2) Close() error {
	return db.f.Close()
}
