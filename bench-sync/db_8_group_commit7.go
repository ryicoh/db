package benchsync

import (
	"encoding/binary"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type DB8 interface {
	Put(key, value []byte) error
	Get(key []byte) ([]byte, error)
}

type db8Impl struct {
	buffer        []byte
	syncBuffer    []byte
	maxBufferSize int
	mutex         sync.Mutex
	rotateCond    *sync.Cond
	syncCond      *sync.Cond
	doneCond      *sync.Cond
	f             *os.File
	cache         map[string][]byte
}

var _ DB8 = &db8Impl{}

type DB8Config struct {
	Path          string
	MaxBufferSize int
	SyncInterval  time.Duration
}

func NewDB8(cfg DB8Config) (*db8Impl, error) {
	f, err := os.OpenFile(cfg.Path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}

	db := &db8Impl{
		buffer:        make([]byte, 0, cfg.MaxBufferSize),
		f:             f,
		maxBufferSize: cfg.MaxBufferSize,
	}
	db.rotateCond = sync.NewCond(&sync.Mutex{})
	db.syncCond = sync.NewCond(&sync.Mutex{})
	db.doneCond = sync.NewCond(&db.mutex)

	go db.rotateWorker(cfg.SyncInterval)
	go db.syncWorker()

	go func() {
		for {
			db.rotateCond.Signal()
			time.Sleep(cfg.SyncInterval)
		}
	}()

	return db, nil
}

func (db *db8Impl) Put(key, value []byte) error {
	pairBytes := pairToBytes(key, value)
	db.mutex.Lock()
	defer db.mutex.Unlock()

	db.buffer = append(db.buffer, pairBytes...)

	// rotate するように通知
	db.rotateCond.Signal()

	// fsync されるまで待機
	db.doneCond.Wait()

	return nil
}

// buffer が 1MB 以上か、100ms 以上経過したら rotate を呼び出す
func (db *db8Impl) rotateWorker(interval time.Duration) {
	lastRotateAt := time.Now()
	for {
		db.rotateCond.L.Lock()
		for len(db.buffer) < db.maxBufferSize &&
			time.Since(lastRotateAt) < interval {
			db.rotateCond.Wait()
		}
		db.rotateCond.L.Unlock()

		db.rotate()
		lastRotateAt = time.Now()
	}
}

// buffer を syncBuffer にコピーして、buffer を空にする
func (db *db8Impl) rotate() {
	db.mutex.Lock()
	defer db.mutex.Unlock()

	db.syncBuffer = db.buffer
	db.buffer = make([]byte, 0, db.maxBufferSize)

	db.syncCond.Signal()
}

// buffer が nil じゃなければ sync を呼び出す
func (db *db8Impl) syncWorker() {
	for {
		db.syncCond.L.Lock()
		// buffer が rotate されるまで待機
		for db.syncBuffer == nil {
			db.syncCond.Wait()
		}

		bufferToSync := db.syncBuffer
		db.syncBuffer = nil
		db.syncCond.L.Unlock()

		db.sync(bufferToSync)
	}
}

var syncTime time.Time

var bufferSize = atomic.Int64{}
var intervalMicros = atomic.Int64{}
var writeMicros = atomic.Int64{}
var writeCount = atomic.Int64{}

// buffer をファイルに書き込んで、待ってる人全員に通知する
func (db *db8Impl) sync(buffer []byte) error {
	{
		if !syncTime.IsZero() {
			intervalMicros.Add(time.Since(syncTime).Microseconds())
		}
		syncTime = time.Now()

		bufferSize.Add(int64(len(buffer)))
	}

	defer func() {
		writeMicros.Add(time.Since(syncTime).Microseconds())
		syncTime = time.Now()
		writeCount.Add(1)
	}()

	if _, err := db.f.Write(buffer); err != nil {
		return err
	}

	//  fmt.Println("syncing", writeCount.Load())
	if err := db.f.Sync(); err != nil {
		return err
	}
	// fmt.Println("synced", writeCount.Load())

	db.doneCond.Broadcast()

	return nil
}

// key と value をバイト列に変換する
func pairToBytes(key, value []byte) []byte {
	buf := make([]byte, keyLenSize+len(key)+valueLenSize+len(value))
	binary.BigEndian.PutUint32(buf[0:keyLenSize], uint32(len(key)))
	copy(buf[keyLenSize:], key)
	binary.BigEndian.PutUint32(buf[keyLenSize+len(key):], uint32(len(value)))
	copy(buf[keyLenSize+len(key)+valueLenSize:], value)
	return buf
}

// Get の処理はめっちゃてきとう
func (db *db8Impl) Get(key []byte) ([]byte, error) {
	if err := db.buildCacheIfEmpty(); err != nil {
		return nil, err
	}

	if value, ok := db.cache[string(key)]; ok {
		return value, nil
	}

	return nil, ErrKeyNotFound
}

func (db *db8Impl) buildCacheIfEmpty() error {
	if db.cache != nil {
		return nil
	}

	db.cache = make(map[string][]byte)

	buf, err := os.ReadFile(db.f.Name())
	if err != nil {
		return err
	}

	offset := 0
	for offset < len(buf) {
		keyLen := binary.BigEndian.Uint32(buf[offset:])
		offset += keyLenSize
		key := buf[offset : offset+int(keyLen)]
		offset += int(keyLen)
		valueLen := binary.BigEndian.Uint32(buf[offset:])
		offset += valueLenSize
		value := buf[offset : offset+int(valueLen)]
		offset += int(valueLen)
		db.cache[string(key)] = value
	}

	return nil
}

func (db *db8Impl) PrintStats() {
	fmt.Printf("writeCount: %d, interval: %dµs, write: %dµs, buffer: %d\n",
		writeCount.Load(),
		intervalMicros.Load()/writeCount.Load(),
		writeMicros.Load()/writeCount.Load(),
		bufferSize.Load()/writeCount.Load(),
	)
}
