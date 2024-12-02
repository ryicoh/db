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
	buffer         []byte
	syncBuffer     []byte
	maxBufferSize  int
	bufferCond     *sync.Cond
	syncBufferCond *sync.Cond
	doneCond       *sync.Cond
	f              *os.File
	cache          map[string][]byte
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
		syncBuffer:    make([]byte, 0, cfg.MaxBufferSize),
		f:             f,
		maxBufferSize: cfg.MaxBufferSize,
	}
	db.bufferCond = sync.NewCond(&sync.Mutex{})
	db.syncBufferCond = sync.NewCond(&sync.Mutex{})
	db.doneCond = sync.NewCond(&sync.Mutex{})

	go db.rotateWorker(cfg.SyncInterval)
	go db.syncWorker()

	go func() {
		for {
			db.bufferCond.Signal()
			time.Sleep(cfg.SyncInterval)
		}
	}()

	return db, nil
}

func (db *db8Impl) Put(key, value []byte) error {
	pairBytes := pairToBytes(key, value)

	db.bufferCond.L.Lock()
	db.buffer = append(db.buffer, pairBytes...)
	db.bufferCond.L.Unlock()

	// rotate するように通知
	db.bufferCond.Signal()

	// fsync されるまで待機
	db.doneCond.L.Lock()
	db.doneCond.Wait()
	db.doneCond.L.Unlock()

	return nil
}

// buffer が 1MB 以上か、100ms 以上経過したら rotate を呼び出す
func (db *db8Impl) rotateWorker(interval time.Duration) {
	lastRotateAt := time.Now()
	for {
		db.bufferCond.L.Lock()
		for len(db.buffer) < db.maxBufferSize &&
			time.Since(lastRotateAt) < interval {
			db.bufferCond.Wait()
		}
		db.bufferCond.L.Unlock()

		db.rotate()
		lastRotateAt = time.Now()
	}
}

// buffer を syncBuffer にコピーして、buffer を空にする
func (db *db8Impl) rotate() {
	db.bufferCond.L.Lock()
	db.syncBufferCond.L.Lock()
	defer db.bufferCond.L.Unlock()
	defer db.syncBufferCond.L.Unlock()
	defer db.syncBufferCond.Signal()

	if len(db.buffer) == 0 {
		return
	}

	for len(db.syncBuffer) > 0 {
		db.syncBufferCond.Wait()
	}

	if len(db.buffer) < cap(db.syncBuffer) {
		db.syncBuffer = db.syncBuffer[:len(db.buffer)]
	} else {
		db.syncBuffer = make([]byte, len(db.buffer))
	}

	copy(db.syncBuffer, db.buffer)
	db.buffer = db.buffer[:0]
}

// buffer が nil じゃなければ sync を呼び出す
func (db *db8Impl) syncWorker() {
	for {
		db.syncBufferCond.L.Lock()
		// buffer が rotate されるまで待機
		for len(db.syncBuffer) == 0 {
			db.syncBufferCond.Wait()
		}

		syncBuffer := db.syncBuffer
		db.syncBuffer = db.syncBuffer[:0]
		db.syncBufferCond.L.Unlock()

		db.syncBufferCond.Signal()

		db.sync(syncBuffer)
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
	fmt.Printf("-- writeCount: %d, interval: %dµs, write: %dµs, buffer: %d\n",
		writeCount.Load(),
		intervalMicros.Load()/writeCount.Load(),
		writeMicros.Load()/writeCount.Load(),
		bufferSize.Load()/writeCount.Load(),
	)
}
