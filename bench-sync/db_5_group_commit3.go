package benchsync

import (
	"context"
	"encoding/binary"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ncw/directio"
)

type syncBlocks struct {
	mu     sync.Mutex
	blocks [][blockSize]byte
}

func (sb *syncBlocks) append(newBlock [blockSize]byte) {
	sb.mu.Lock()
	sb.blocks = append(sb.blocks, newBlock)
	sb.mu.Unlock()
}

func (sb *syncBlocks) flush() [][blockSize]byte {
	sb.mu.Lock()
	blocks := sb.blocks
	sb.blocks = make([][blockSize]byte, 0, len(blocks)/2)
	sb.mu.Unlock()

	return blocks
}

type dbGroupCommit3 struct {
	f      *os.File
	offset int64

	currentBlock       [blockSize]byte
	currentBlockOffset uint32

	cancelSync   context.CancelFunc
	syncBlocks   [][blockSize]byte
	syncInterval time.Duration
	syncCnt      atomic.Int32

	putMutex    sync.Mutex
	rotateMutex sync.Mutex
	syncMutex   sync.Mutex
}

var _ DB = &dbGroupCommit3{}

func OpenDBGroupCommit3(fileName string) (*dbGroupCommit3, error) {
	f, err := directio.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	return &dbGroupCommit3{
		f:            f,
		currentBlock: [blockSize]byte{},
	}, nil
}

func (db *dbGroupCommit3) StartSync() {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		for {
			for db.syncCnt.Load() > 0 {
				db.syncCnt.Add(-1)
				blocks := db.syncBlocks

				db.sync(blocks)
			}

			select {
			case <-ctx.Done():
				return
			case <-time.After(db.syncInterval):
			}
		}
	}()

	db.cancelSync = cancel
}

func (db *dbGroupCommit3) StopSync() {
	db.cancelSync()
}

func (db *dbGroupCommit3) sync(blocks [][blockSize]byte) (err error) {
	for _, block := range blocks {
		if _, err = db.f.WriteAt(block[:], db.offset); err != nil {
			goto notify
		}
		db.offset += int64(blockSize)
	}

	if err = db.f.Sync(); err != nil {
		goto notify
	}

notify:
	db.notifyError(err)
	return
}

func (db *dbGroupCommit3) notifyError(err error) {
	panic(err)
}

func (db *dbGroupCommit3) rotateBlock() {
	db.rotateMutex.Lock()
	db.syncBlocks = append(db.syncBlocks, db.currentBlock)
	db.currentBlock = [blockSize]byte{}
	db.currentBlockOffset = 0
	db.rotateMutex.Unlock()
}

func (db *dbGroupCommit3) Put(key, value []byte) error {
	db.putConcurrently(key, value)

	block := make([]byte, blockSize) // FIXME
	copy(block, db.currentBlock[:])
	db.putMutex.Unlock()

	return nil
}

func (db *dbGroupCommit3) putConcurrently(key, value []byte) {
	keyLen := uint32(len(key))
	recordSize := uint32(keyLenSize + len(key) + valueLenSize + len(value))

	db.putMutex.Lock()
	{
		currentBlockOffset := db.currentBlockOffset
		if currentBlockOffset+recordSize > blockSize {
			db.rotateBlock()
			currentBlockOffset = 0
		}
		db.currentBlockOffset = currentBlockOffset + recordSize
	}
	db.putMutex.Unlock()

	{
		binary.BigEndian.PutUint32(db.currentBlock[db.currentBlockOffset:], keyLen)
		copy(db.currentBlock[db.currentBlockOffset+keyLenSize:], key)
	}
	{
		valueOffset := keyLenSize + keyLen
		binary.BigEndian.PutUint32(db.currentBlock[db.currentBlockOffset+valueOffset:], uint32(len(value)))
		copy(db.currentBlock[db.currentBlockOffset+valueOffset+valueLenSize:], value)
	}
}

func (db *dbGroupCommit3) Get(key []byte) ([]byte, error) {
	panic("not implemented")
}

func (db *dbGroupCommit3) Close() error {
	return db.f.Close()
}
