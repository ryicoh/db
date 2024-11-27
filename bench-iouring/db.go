package benchiouring

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"os"

	"github.com/iceber/iouring-go"
)

type DB interface {
	Put(key, value []byte) error
	Get(key []byte) ([]byte, error)
}

type PutTask struct {
	data  []byte
	errCh chan error
}

type SubmitTask struct {
	buf   []byte
	errCh []chan error
}

type NotifyTask struct {
	errCh []chan error
	err   error
}

type db struct {
	f      *os.File
	iour   *iouring.IOURing
	offset uint64

	putTaskQueue chan PutTask
	maxBatchSize int

	submitTaskQueue chan SubmitTask
	notifyTaskQueue chan NotifyTask

	putTaskQueueLenSum int
	putTaskQueueCnt    int

	submitTaskQueueLenSum int
	submitTaskQueueCnt    int

	notifyTaskQueueLenSum int
	notifyTaskQueueCnt    int

	cancel context.CancelFunc

	cache map[string][]byte
}

var _ DB = &db{}

type DBConfig struct {
	Entries         uint
	PutQueueSize    int
	SubmitQueueSize int
	MaxBatchSize    int
}

func OpenDB(path string, cfg DBConfig) (*db, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}

	return NewDB(f, cfg), nil
}

func NewDB(f *os.File, cfg DBConfig) *db {
	iour, err := iouring.New(cfg.Entries)
	if err != nil {
		panic(err)
	}

	db := &db{
		f:               f,
		iour:            iour,
		putTaskQueue:    make(chan PutTask, cfg.PutQueueSize),
		submitTaskQueue: make(chan SubmitTask, cfg.SubmitQueueSize),
		notifyTaskQueue: make(chan NotifyTask, 16),
		maxBatchSize:    cfg.MaxBatchSize,
	}

	return db
}

func (d *db) Start(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	d.cancel = cancel

	go d.batchPutLoop(ctx)
	go d.submitLoop(ctx)
	go d.notifyLoop(ctx)
}

func (d *db) Put(key, value []byte) error {
	errCh := make(chan error, 1)
	d.putTaskQueueLenSum += len(d.putTaskQueue)
	d.putTaskQueueCnt++
	d.putTaskQueue <- PutTask{
		data:  d.tupleToBuffer(key, value),
		errCh: errCh,
	}
	return <-errCh
}

func (d *db) batchPutLoop(ctx context.Context) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case firstTask := <-d.putTaskQueue:
				tasks := make([]PutTask, 0, cap(d.putTaskQueue))
				tasks = append(tasks, firstTask)

				bufSize := len(firstTask.data)
				for {
					select {
					case task := <-d.putTaskQueue:
						tasks = append(tasks, task)
						bufSize += len(task.data)
						if bufSize > d.maxBatchSize {
							goto submit
						}
					default:
						goto submit
					}
				}

			submit:
				errChList := make([]chan error, len(tasks))
				buf := make([]byte, bufSize)
				offset := 0
				for i, task := range tasks {
					errChList[i] = task.errCh
					copy(buf[offset:], task.data)
					offset += len(task.data)
				}

				d.submitTaskQueueLenSum += len(d.submitTaskQueue)
				d.submitTaskQueueCnt++
				d.submitTaskQueue <- SubmitTask{
					buf:   buf,
					errCh: errChList,
				}
			}
		}
	}()
}

func (d *db) submitLoop(ctx context.Context) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case firstTask := <-d.submitTaskQueue:
				tasks := make([]SubmitTask, 0, cap(d.submitTaskQueue))
				tasks = append(tasks, firstTask)

				for {
					select {
					case task := <-d.submitTaskQueue:
						tasks = append(tasks, task)
					default:
						goto submit
					}
				}

			submit:
				errChs := make([]chan error, 0, len(tasks))
				for _, task := range tasks {
					errChs = append(errChs, task.errCh...)
				}
				d.submit(tasks, errChs)
			}
		}
	}()
}

func (d *db) submit(tasks []SubmitTask, errChs []chan error) {
	var err error
	defer d.enqueueNotifyTask(errChs, err)

	req := make([]iouring.PrepRequest, len(tasks))
	for i := range req {
		req[i] = iouring.Pwrite(int(d.f.Fd()), tasks[i].buf, d.offset)
		d.offset += uint64(len(tasks[i].buf))
	}
	res, err := d.iour.SubmitRequests(req, nil)
	if err != nil {
		return
	}

	<-res.Done()
}

func (d *db) notifyLoop(ctx context.Context) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case task := <-d.notifyTaskQueue:
				d.notify(task)
			}
		}
	}()
}

func (d *db) notify(task NotifyTask) {
	for _, errCh := range task.errCh {
		errCh <- task.err
	}
}

func (d *db) enqueueNotifyTask(errChs []chan error, err error) {
	d.notifyTaskQueueLenSum += len(d.notifyTaskQueue)
	d.notifyTaskQueueCnt++
	d.notifyTaskQueue <- NotifyTask{
		errCh: errChs,
		err:   err,
	}
}

var ErrKeyNotFound = fmt.Errorf("key not found")
var keyEncoder = base64.StdEncoding

func (d *db) Get(key []byte) ([]byte, error) {
	if err := d.buildCacheIfEmpty(); err != nil {
		return nil, err
	}

	if value, ok := d.cache[keyEncoder.EncodeToString(key)]; ok {
		return value, nil
	}
	return nil, ErrKeyNotFound
}

const keyLenSize = 4
const valueLenSize = 4

func (db *db) tupleToBuffer(key, value []byte) []byte {
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

func (d *db) buildCacheIfEmpty() error {
	if d.cache != nil {
		return nil
	}

	d.cache = make(map[string][]byte)

	buf, err := os.ReadFile(d.f.Name())
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
		d.cache[keyEncoder.EncodeToString(key)] = value
	}

	return nil
}

func (d *db) Stats() string {
	return fmt.Sprintf("put:%d|%d/%d, submit:%d|%d/%d, notify:%d|%d/%d",
		d.putTaskQueueCnt,
		d.putTaskQueueLenSum/d.putTaskQueueCnt,
		cap(d.putTaskQueue),
		d.submitTaskQueueCnt,
		d.submitTaskQueueLenSum/d.submitTaskQueueCnt,
		cap(d.submitTaskQueue),
		d.notifyTaskQueueCnt,
		d.notifyTaskQueueLenSum/d.notifyTaskQueueCnt,
		cap(d.notifyTaskQueue),
	)
}
