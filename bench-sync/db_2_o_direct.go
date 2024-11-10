package benchsync

import (
	"encoding/binary"
	"os"
	"sync"

	"github.com/ncw/directio"
)

type dbODirect struct {
	mu                 sync.Mutex
	f                  *os.File
	offset             int64
	currentBlock       []byte
	currentBlockOffset uint32
}

const blockSize = directio.BlockSize

var _ DB = &dbODirect{}

func OpenDBODirect(fileName string) (*dbODirect, error) {
	f, err := directio.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	return &dbODirect{
		f:                  f,
		offset:             0,
		currentBlock:       newBlock(), // FIXME: reuse buffer if it possible
		currentBlockOffset: 0,
	}, nil
}

func (db *dbODirect) rotateBlock() {
	db.currentBlock = newBlock()
	db.currentBlockOffset = 0
	db.offset += blockSize
}

func (db *dbODirect) Put(key, value []byte) error {
	db.mu.Lock()
	defer db.mu.Unlock()

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

	if err := db.f.Sync(); err != nil {
		return err
	}

	return nil
}

func (db *dbODirect) Get(key []byte) ([]byte, error) {
	panic("not implemented")
}

func (db *dbODirect) Close() error {
	return db.f.Close()
}

func newBlock() []byte {
	return make([]byte, blockSize)
}

func (db *dbODirect) DebugRotateBlock() {
	db.rotateBlock()
}
