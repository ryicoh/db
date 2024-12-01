package benchsync

import (
	"encoding/binary"
	"fmt"
	"os"
	"sync"
	"time"
)

type DB9 interface {
	Put(key, value []byte) error
	Get(key []byte) ([]byte, error)
}

type db9Impl struct {
	buffer        []byte
	syncBuffer    []byte
	maxBufferSize int
	mux           sync.Mutex
	syncCond      *sync.Cond
	doneCond      *sync.Cond
	f             *os.File
	cache         map[string][]byte
}

var _ DB9 = &db9Impl{}

type DB9Config struct {
	Path          string
	MaxBufferSize int
	SyncInterval  time.Duration
}

func NewDB9(cfg DB9Config) (*db9Impl, error) {
	f, err := os.OpenFile(cfg.Path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}

	db := &db9Impl{
		buffer:        make([]byte, 0, cfg.MaxBufferSize),
		syncBuffer:    make([]byte, 0, cfg.MaxBufferSize),
		f:             f,
		maxBufferSize: cfg.MaxBufferSize,
	}
	db.syncCond = sync.NewCond(&sync.Mutex{})
	db.doneCond = sync.NewCond(&sync.Mutex{})

	go db.syncWorker()

	go func() {
		for {
			time.Sleep(cfg.SyncInterval)

			db.mux.Lock()
			db.rotate()
			db.mux.Unlock()

		}
	}()

	return db, nil
}

func (db *db9Impl) Put(key, value []byte) error {
	pairBytes := pairToBytes(key, value)
	db.append(pairBytes)

	db.wait()

	return nil
}

func (db *db9Impl) append(data []byte) {
	db.mux.Lock()
	defer db.mux.Unlock()

	fmt.Printf("%v <= %v\n", db.buffer, data)

	// もし、追加するとあふれるなら rotate する
	if len(db.buffer)+len(data) > db.maxBufferSize {
		db.rotate()
	}

	db.buffer = append(db.buffer, data...)
	fmt.Printf("buffer: %v\n", db.buffer)
}

func (db *db9Impl) wait() {
	db.doneCond.L.Lock()
	defer db.doneCond.L.Unlock()
	db.doneCond.Wait()
}

// buffer を syncBuffer にコピーして、buffer を空にする
func (db *db9Impl) rotate() {
	if len(db.buffer) == 0 {
		return
	}

	db.syncCond.L.Lock()
	{
		// db.syncBuffer が0 になるまで待つ
		for len(db.syncBuffer) > 0 {
			db.syncCond.Wait()
		}

		db.syncBuffer = db.syncBuffer[:len(db.buffer)]
		copy(db.syncBuffer, db.buffer)
		fmt.Printf("rotated,syncBuffer: %v\n", db.syncBuffer)
	}
	db.syncCond.L.Unlock()

	db.buffer = db.buffer[:0]

	db.syncCond.Signal()
}

// buffer が nil じゃなければ sync を呼び出す
func (db *db9Impl) syncWorker() {
	for {
		db.syncCond.L.Lock()
		// syncBuffer が nil でなくなるまで待機
		for len(db.syncBuffer) == 0 {
			db.syncCond.Wait()
		}

		bufferToSync := make([]byte, len(db.syncBuffer))
		copy(bufferToSync, db.syncBuffer)
		db.syncBuffer = db.syncBuffer[:0]

		db.syncCond.L.Unlock()

		db.sync(bufferToSync)
	}
}

// buffer をファイルに書き込んで、待ってる人全員に通知する
func (db *db9Impl) sync(buffer []byte) error {
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

	if err := db.f.Sync(); err != nil {
		return err
	}

	db.doneCond.Broadcast()

	return nil
}

// Get の処理はめっちゃてきとう
func (db *db9Impl) Get(key []byte) ([]byte, error) {
	if err := db.buildCacheIfEmpty(); err != nil {
		return nil, err
	}

	if value, ok := db.cache[string(key)]; ok {
		return value, nil
	}

	return nil, ErrKeyNotFound
}

func (db *db9Impl) buildCacheIfEmpty() error {
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

func (db *db9Impl) PrintStats() {
	fmt.Printf("  writeCount: %d, interval: %dµs, write: %dµs, buffer: %d\n",
		writeCount.Load(),
		intervalMicros.Load()/writeCount.Load(),
		writeMicros.Load()/writeCount.Load(),
		bufferSize.Load()/writeCount.Load(),
	)
}
