package benchsync

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"log/slog"
	"os"
	"sync/atomic"
	"time"
)

type putTask struct {
	key   []byte
	value []byte
	errCh chan error
}

type writeTask struct {
	buffer []byte
	errChs []chan error
}

type notifyTask struct {
	errChs []chan error
	err    error
}

type dbGroupCommit4 struct {
	f *os.File

	putQueueCapacity    int
	maxBufferLen        int
	writeQueueCapacity  int
	notifyQueueCapacity int
	putQueue            chan putTask
	writeQueue          chan writeTask
	notifyQueue         chan notifyTask

	cancel context.CancelFunc

	cache map[string][]byte

	numPut   atomic.Uint64
	numWrite uint64
	// putQueueLenBeforeEnqueue       []float64
	// putQueueLenBeforeEnqueueMux    sync.Mutex
	// writeQueueLenBeforeEnqueue     []float64
	// writeQueueLenBeforeEnqueueMux  sync.Mutex
	// notifyQueueLenBeforeEnqueue    []float64
	// notifyQueueLenBeforeEnqueueMux sync.Mutex
	writeElapsedNano      uint64
	writeInterval1Nano    uint64
	writeInterval2Nano    uint64
	writeInterval3Nano    uint64
	writePreviousTimeNano uint64
}

var _ DB = (*dbGroupCommit4)(nil)

type DBGroupCommit4Config struct {
	PutQueueCapacity    int
	MaxBufferLen        int
	WriteQueueCapacity  int
	NotifyQueueCapacity int
}

func OpenDBGroupCommit4(path string, cfg DBGroupCommit4Config) (*dbGroupCommit4, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	return &dbGroupCommit4{
		f:                   f,
		maxBufferLen:        cfg.MaxBufferLen,
		putQueueCapacity:    cfg.PutQueueCapacity,
		writeQueueCapacity:  cfg.WriteQueueCapacity,
		notifyQueueCapacity: cfg.NotifyQueueCapacity,
		putQueue:            make(chan putTask, cfg.PutQueueCapacity),
		writeQueue:          make(chan writeTask, cfg.WriteQueueCapacity),
		notifyQueue:         make(chan notifyTask, cfg.NotifyQueueCapacity),
		// putQueueLenBeforeEnqueue:    make([]float64, 0, cfg.PutQueueCapacity),
		// writeQueueLenBeforeEnqueue:  make([]float64, 0, cfg.WriteQueueCapacity),
		// notifyQueueLenBeforeEnqueue: make([]float64, 0, cfg.NotifyQueueCapacity),
	}, nil
}

func (db *dbGroupCommit4) StartWorkers(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	db.cancel = cancel

	go db.putLoop(ctx)
	go db.writeLoop(ctx)
	go db.notifyLoop(ctx)
}

func (db *dbGroupCommit4) putLoop(ctx context.Context) {
	defer close(db.putQueue)

	for {
		fmt.Println("putQueue", len(db.putQueue))

		select {
		case <-ctx.Done():
			return
		case firstTask := <-db.putQueue:
			errChs := make([]chan error, 0, db.putQueueCapacity)
			errChs = append(errChs, firstTask.errCh)

			buffer := make([]byte, 0, db.maxBufferLen)
			// buffer := *(db.pool.Get().(*[]byte))
			buffer = append(buffer, db.tupleToBuffer(firstTask.key, firstTask.value)...)

			// after := time.After(32 * time.Microsecond)

			// dequeue tasks from putQueue until the queue is empty or the batch is full
			for len(buffer) < int(db.maxBufferLen) {
				select {
				case <-ctx.Done():
					return
				// case <-after:
				// goto end
				case task := <-db.putQueue:
					buffer = append(buffer, db.tupleToBuffer(task.key, task.value)...)
					errChs = append(errChs, task.errCh)
				default:
					goto end
				}
			}
		end:

			slog.Debug("queue len", "putQueue", len(db.putQueue), "writeQueue", len(db.writeQueue), "notifyQueue", len(db.notifyQueue))

			// db.writeQueueLenBeforeEnqueueMux.Lock()
			// db.writeQueueLenBeforeEnqueue = append(db.writeQueueLenBeforeEnqueue, float64(len(db.writeQueue)))
			// db.writeQueueLenBeforeEnqueueMux.Unlock()
			// enqueue the buffer to the writeQueue
			db.writeQueue <- writeTask{
				buffer: buffer,
				errChs: errChs,
			}
		}
	}
}

func (db *dbGroupCommit4) tupleToBuffer(key, value []byte) []byte {
	keyLen := uint32(len(key))
	recordSize := uint32(keyLenSize + len(key) + valueLenSize + len(value))
	buffer := make([]byte, recordSize)

	{
		binary.BigEndian.PutUint32(buffer[0:], keyLen)
		copy(buffer[keyLenSize:], key)
	}
	{
		valueOffset := keyLenSize + keyLen
		binary.BigEndian.PutUint32(buffer[valueOffset:], uint32(len(value)))
		copy(buffer[valueOffset+valueLenSize:], value)
	}

	return buffer
}

func (db *dbGroupCommit4) Put(key, value []byte) error {
	errCh := make(chan error)

	// db.putQueueLenBeforeEnqueueMux.Lock()
	// db.putQueueLenBeforeEnqueue = append(db.putQueueLenBeforeEnqueue, float64(len(db.putQueue)))
	// db.putQueueLenBeforeEnqueueMux.Unlock()
	db.putQueue <- putTask{key: key, value: value, errCh: errCh}

	err := <-errCh

	db.numPut.Add(1)

	return err
}

func (db *dbGroupCommit4) writeLoop(ctx context.Context) {
	defer close(db.writeQueue)
	for {
		{
			now := uint64(time.Now().UnixNano())
			prev := db.writePreviousTimeNano
			if prev > 0 {
				db.writeInterval2Nano += now - prev
			}
			db.writePreviousTimeNano = now
		}

		fmt.Println("writeQueue", len(db.writeQueue))

		select {
		case <-ctx.Done():
			return
		case task := <-db.writeQueue:
			{
				now := uint64(time.Now().UnixNano())
				prev := db.writePreviousTimeNano
				if prev > 0 {
					db.writeInterval3Nano += now - prev
				}
				db.writePreviousTimeNano = now
			}
			db.write(task)
		}
	}
}

func (db *dbGroupCommit4) write(task writeTask) {
	{
		now := uint64(time.Now().UnixNano())
		prev := db.writePreviousTimeNano
		if prev > 0 {
			db.writeInterval1Nano += now - prev
		}
		db.writePreviousTimeNano = now
	}

	{
		defer func() {
			now := uint64(time.Now().UnixNano())
			prev := db.writePreviousTimeNano
			if prev > 0 {
				db.writeElapsedNano += now - prev
			}
			db.writePreviousTimeNano = now
		}()
	}

	db.numWrite += 1

	var err error
	defer db.enqueueNotify(task.errChs, err)

	_, err = db.f.Write(task.buffer)
	if err != nil {
		return
	}

	err = db.f.Sync()
	if err != nil {
		return
	}
}

func (db *dbGroupCommit4) notifyLoop(ctx context.Context) {
	defer close(db.notifyQueue)

	for {
		select {
		case <-ctx.Done():
			return
		case task := <-db.notifyQueue:
			db.notifyErr(task)
		}
	}
}

func (db *dbGroupCommit4) enqueueNotify(errChs []chan error, err error) {
	// db.notifyQueueLenBeforeEnqueueMux.Lock()
	// db.notifyQueueLenBeforeEnqueue = append(db.notifyQueueLenBeforeEnqueue, float64(len(db.notifyQueue)))
	// db.notifyQueueLenBeforeEnqueueMux.Unlock()
	db.notifyQueue <- notifyTask{errChs: errChs, err: err}
}

func (db *dbGroupCommit4) notifyErr(task notifyTask) {
	for _, errCh := range task.errChs {
		errCh <- task.err
	}
}

func (db *dbGroupCommit4) Close() error {
	db.cancel()
	return db.f.Close()
}

var keyEncoder = base64.StdEncoding
var ErrKeyNotFound = fmt.Errorf("key not found")

func (db *dbGroupCommit4) Get(key []byte) ([]byte, error) {
	if err := db.buildCacheIfEmpty(); err != nil {
		return nil, err
	}

	if value, ok := db.cache[keyEncoder.EncodeToString(key)]; ok {
		return value, nil
	}

	return nil, ErrKeyNotFound
}

func (db *dbGroupCommit4) buildCacheIfEmpty() error {
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
		db.cache[keyEncoder.EncodeToString(key)] = value
	}

	return nil
}

func (db *dbGroupCommit4) Stats() string {
	numPut := db.numPut.Load()

	// percentile := 90.0
	// putQueueLenBeforeEnqueue, err := stats.Percentile(db.putQueueLenBeforeEnqueue, percentile)
	// if err != nil {
	// 	panic(err)
	// }
	// writeQueueLenBeforeEnqueue, err := stats.Percentile(db.writeQueueLenBeforeEnqueue, percentile)
	// if err != nil {
	// 	panic(err)
	// }
	// notifyQueueLenBeforeEnqueue, err := stats.Percentile(db.notifyQueueLenBeforeEnqueue, percentile)
	// if err != nil {
	// 	panic(err)
	// }

	// return fmt.Sprintf(`put:%d|%d/%d, write:%d|%d/%d, notify:%d/%d, write:%s, interval:%s`,
	// int(math.Round(putQueueLenBeforeEnqueue)), db.putQueueCapacity,
	// int(math.Round(writeQueueLenBeforeEnqueue)), db.writeQueueCapacity,
	// int(math.Round(notifyQueueLenBeforeEnqueue)), db.notifyQueueCapacity,
	return fmt.Sprintf(`put:%d, write:%d, write:%s, interval1:%s, interval2:%s, interval3:%s`,
		numPut,
		db.numWrite,
		time.Duration(db.writeElapsedNano/db.numWrite),
		time.Duration(db.writeInterval1Nano/db.numWrite),
		time.Duration(db.writeInterval2Nano/db.numWrite),
		time.Duration(db.writeInterval3Nano/db.numWrite),
	)
}
