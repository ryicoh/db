package a002bitcask

import "sync"

type InMemorySortedMap interface {
	Put(key, value []byte)
	Get(key []byte) ([]byte, bool)
	Has(key []byte) bool
	Len() int
	Delete(key []byte) bool
	Ascend(from []byte, fn func(key, value []byte) bool)
	Descend(from []byte, fn func(key, value []byte) bool)
}

type InMemoryMap[V any] interface {
	Put(key []byte, value V)
	Get(key []byte) (V, bool)
	Has(key []byte) bool
	Len() int
	Delete(key []byte) bool
}

type segmentPointer struct {
	segmentId int
	offset    int
}

type DB struct {
	mu             sync.Mutex
	dir            string
	index          InMemoryMap[segmentPointer]
	segmentId      int
	segmentMap     InMemorySortedMap
	segmentSize    int
	maxSegmentSize int
}

func NewDB(dir string) *DB {
	return &DB{
		dir:            dir,
		index:          newInMemoryMap[segmentPointer](),
		segmentId:      1,
		segmentMap:     newInMemorySortedMap(),
		segmentSize:    int(headerSize),
		maxSegmentSize: 1024 * 1024, // 1MB
	}
}

func (db *DB) Put(key, value []byte) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	size := offsetSize + len(key) + len(value)
	if db.segmentSize+size > db.maxSegmentSize {
		db.updateSegment()
	}

	db.segmentMap.Put(key, value)
	db.segmentSize += size

	db.index.Put(key, segmentPointer{
		segmentId: db.segmentId,
		offset:    "???"
	})
	return nil
}

func (db *DB) Get(key []byte) ([]byte, error) {
	panic("not implemented")
}

func (db *DB) Delete(key []byte) error {
	panic("not implemented")
}

func (db *DB) updateSegment() {
	panic("not implemented")
}
