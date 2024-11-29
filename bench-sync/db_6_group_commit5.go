package benchsync

import (
	"context"
	"encoding/binary"
	"fmt"
	"log/slog"
	"os"
	"sync/atomic"
	"time"
	"unsafe"
)

type Meta struct {
	KeyLen   uint32
	ValueLen uint32
}

const metaLenSize = int(unsafe.Sizeof(Meta{}))

type Item struct {
	Key   []byte
	Value []byte
}

func UnsafeMetaToBuffer(meta *Meta) []byte {
	return (*[unsafe.Sizeof(*meta)]byte)(unsafe.Pointer(meta))[:]
}

func UnsafeBufferToMeta(buffer []byte) *Meta {
	return (*Meta)(unsafe.Pointer(&buffer[0]))
}

type putTask2 struct {
	item  *Item
	errCh chan error
}

type writeTask2 struct {
	buffer []byte
	errChs []chan error
}

type notifyTask2 struct {
	errChs []chan error
	err    error
}

type dbGroupCommit5 struct {
	f *os.File

	putQueueCapacity    int
	maxBufferLen        int
	writeQueueCapacity  int
	notifyQueueCapacity int
	putQueue            chan putTask2
	writeQueue          chan writeTask2
	notifyQueue         chan notifyTask2

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

type DBGroupCommit5Config struct {
	PutQueueCapacity    int
	MaxBufferLen        int
	WriteQueueCapacity  int
	NotifyQueueCapacity int
}

func OpenDBGroupCommit5(path string, cfg DBGroupCommit5Config) (*dbGroupCommit5, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	return &dbGroupCommit5{
		f:                   f,
		maxBufferLen:        cfg.MaxBufferLen,
		putQueueCapacity:    cfg.PutQueueCapacity,
		writeQueueCapacity:  cfg.WriteQueueCapacity,
		notifyQueueCapacity: cfg.NotifyQueueCapacity,
		putQueue:            make(chan putTask2, cfg.PutQueueCapacity),
		writeQueue:          make(chan writeTask2, cfg.WriteQueueCapacity),
		notifyQueue:         make(chan notifyTask2, cfg.NotifyQueueCapacity),
		// putQueueLenBeforeEnqueue:    make([]float64, 0, cfg.PutQueueCapacity),
		// writeQueueLenBeforeEnqueue:  make([]float64, 0, cfg.WriteQueueCapacity),
		// notifyQueueLenBeforeEnqueue: make([]float64, 0, cfg.NotifyQueueCapacity),
	}, nil
}

func (db *dbGroupCommit5) StartWorkers(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	db.cancel = cancel

	go db.putLoop(ctx)
	go db.writeLoop(ctx)
	go db.notifyLoop(ctx)
}

func (db *dbGroupCommit5) putLoop(ctx context.Context) {
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
			buffer = append(buffer, db.tupleToBuffer(firstTask.item)...)

			// after := time.After(32 * time.Microsecond)

			// dequeue tasks from putQueue until the queue is empty or the batch is full
			for len(buffer) < int(db.maxBufferLen) {
				select {
				case <-ctx.Done():
					return
				// case <-after: // goto end
				case task := <-db.putQueue:
					buffer = append(buffer, db.tupleToBuffer(task.item)...)
					errChs = append(errChs, task.errCh)
				default:
					goto end
				}
			}
		end:
			if len(buffer) > int(db.maxBufferLen)/5 {
				fmt.Printf("buffer ratio: %0.2f\n", float64(len(buffer))/float64(db.maxBufferLen))
			}

			slog.Debug("queue len", "putQueue", len(db.putQueue), "writeQueue", len(db.writeQueue), "notifyQueue", len(db.notifyQueue))

			// db.writeQueueLenBeforeEnqueueMux.Lock()
			// db.writeQueueLenBeforeEnqueue = append(db.writeQueueLenBeforeEnqueue, float64(len(db.writeQueue)))
			// db.writeQueueLenBeforeEnqueueMux.Unlock()
			// enqueue the buffer to the writeQueue
			db.writeQueue <- writeTask2{
				buffer: buffer,
				errChs: errChs,
			}
		}
	}
}

func (db *dbGroupCommit5) tupleToBuffer(item *Item) []byte {
	buf := make([]byte, metaLenSize+len(item.Key)+len(item.Value))
	copy(buf, UnsafeMetaToBuffer(&Meta{
		KeyLen:   uint32(len(item.Key)),
		ValueLen: uint32(len(item.Value)),
	}))
	copy(buf[metaLenSize:], item.Key)
	copy(buf[metaLenSize+len(item.Key):], item.Value)
	return buf
}

func (db *dbGroupCommit5) Put(item *Item) error {
	errCh := make(chan error)

	// db.putQueueLenBeforeEnqueueMux.Lock()
	// db.putQueueLenBeforeEnqueue = append(db.putQueueLenBeforeEnqueue, float64(len(db.putQueue)))
	// db.putQueueLenBeforeEnqueueMux.Unlock()
	db.putQueue <- putTask2{item: item, errCh: errCh}

	err := <-errCh

	db.numPut.Add(1)

	return err
}

func (db *dbGroupCommit5) writeLoop(ctx context.Context) {
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
		case task := <-db.writeQueue:
			{
				now := uint64(time.Now().UnixNano())
				prev := db.writePreviousTimeNano
				if prev > 0 {
					db.writeInterval3Nano += now - prev
				}
				db.writePreviousTimeNano = now
			}
			_ = task
			// db.write(task)
			db.numWrite += 1
			db.enqueueNotify(task.errChs, nil)
		case <-ctx.Done():
			return
		}
	}
}

func (db *dbGroupCommit5) write(task writeTask2) {
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

func (db *dbGroupCommit5) notifyLoop(ctx context.Context) {
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

func (db *dbGroupCommit5) enqueueNotify(errChs []chan error, err error) {
	// db.notifyQueueLenBeforeEnqueueMux.Lock()
	// db.notifyQueueLenBeforeEnqueue = append(db.notifyQueueLenBeforeEnqueue, float64(len(db.notifyQueue)))
	// db.notifyQueueLenBeforeEnqueueMux.Unlock()
	db.notifyQueue <- notifyTask2{errChs: errChs, err: err}
}

func (db *dbGroupCommit5) notifyErr(task notifyTask2) {
	for _, errCh := range task.errChs {
		errCh <- task.err
	}
}

func (db *dbGroupCommit5) Close() error {
	db.cancel()
	return db.f.Close()
}

func (db *dbGroupCommit5) Get(key []byte) ([]byte, error) {
	if err := db.buildCacheIfEmpty(); err != nil {
		return nil, err
	}

	if value, ok := db.cache[keyEncoder.EncodeToString(key)]; ok {
		return value, nil
	}

	return nil, ErrKeyNotFound
}

func (db *dbGroupCommit5) buildCacheIfEmpty() error {
	if db.cache != nil {
		return nil
	}

	db.cache = make(map[string][]byte)

	buf, err := os.ReadFile(db.f.Name())
	if err != nil {
		return err
	}
	fmt.Println("buf", len(buf))

	offset := 0
	for offset < len(buf) {
		keyLen := binary.LittleEndian.Uint32(buf[offset:])
		offset += keyLenSize
		key := buf[offset : offset+int(keyLen)]
		offset += int(keyLen)
		valueLen := binary.LittleEndian.Uint32(buf[offset:])
		offset += valueLenSize
		value := buf[offset : offset+int(valueLen)]
		offset += int(valueLen)
		db.cache[keyEncoder.EncodeToString(key)] = value
	}

	return nil
}

func (db *dbGroupCommit5) Stats() string {
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
