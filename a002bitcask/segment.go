package a002bitcask

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"unsafe"
)

var ErrKeyNotFound = errors.New("key not found")

// `cell` is the smallest unit of data in a segment.
// It consists of a header and a key/value pair.
// For example, if a key is "foo" and a value is "bar", the cell is as follows:
// |             header              | key | value |
// | keySize | valueSize | tombstone |     |       |
// |   3     |    3      |     1     | foo |  bar  |

type cellHeader struct {
	keySize   uint32
	valueSize uint32
	tombstone bool
}

const cellHeaderSize = int(unsafe.Sizeof(cellHeader{}))

func (h *cellHeader) unmarshal(data [cellHeaderSize]byte) {
	copy(unsafe.Slice((*byte)(unsafe.Pointer(h)), cellHeaderSize), data[:])
}

func (h *cellHeader) marshal() [cellHeaderSize]byte {
	return *(*[cellHeaderSize]byte)(unsafe.Pointer(h))
}

type InMemoryMap[V any] interface {
	Put(key []byte, value V)
	Get(key []byte) (V, bool)
	Has(key []byte) bool
	Len() int
	Delete(key []byte) bool
	GetAllKeys() [][]byte
}

type file interface {
	io.WriterAt
	io.ReaderAt
	io.Closer
}

// `Segment` is a file that stores cells.
// Internally, it uses a hash map to store the mapping from keys to cell offsets.
type Segment struct {
	mux     sync.Mutex
	hashMap InMemoryMap[int64]
	file    file
	offset  int64
}

func NewSegment(dir string, segmentId int) (*Segment, error) {
	file, err := openFile(dir, segmentId)
	if err != nil {
		return nil, err
	}

	stat, err := file.Stat()
	if err != nil {
		return nil, err
	}

	seg := &Segment{
		hashMap: newInMemoryMap[int64](),
		file:    file,
		offset:  stat.Size(),
	}

	if err := seg.restoreHashMap(); err != nil {
		return nil, err
	}
	return seg, nil
}

func (seg *Segment) Close() error {
	return seg.file.Close()
}

func (seg *Segment) Put(key, value []byte) (replaced bool, err error) {
	seg.mux.Lock()
	defer seg.mux.Unlock()

	currentOffset := seg.offset

	// write header
	{
		header := cellHeader{
			keySize:   uint32(len(key)),
			valueSize: uint32(len(value)),
		}
		headerBytes := header.marshal()
		if _, err := seg.file.WriteAt(headerBytes[:], seg.offset); err != nil {
			return false, err
		}
		seg.offset += int64(cellHeaderSize)
	}

	// write key
	{
		if _, err := seg.file.WriteAt(key, seg.offset); err != nil {
			return false, err
		}
		seg.offset += int64(len(key))
	}

	// write value
	{
		if _, err := seg.file.WriteAt(value, seg.offset); err != nil {
			return false, err
		}
		seg.offset += int64(len(value))
	}

	// put to hash map
	replaced = seg.hashMap.Has(key)
	seg.hashMap.Put(key, currentOffset)

	return replaced, nil
}

func (seg *Segment) Delete(key []byte) error {
	seg.mux.Lock()
	defer seg.mux.Unlock()

	found := seg.hashMap.Has(key)
	if !found {
		return ErrKeyNotFound
	}

	// write header
	{
		header := cellHeader{
			keySize:   uint32(len(key)),
			valueSize: 0,
			tombstone: true,
		}
		headerBytes := header.marshal()
		if _, err := seg.file.WriteAt(headerBytes[:], seg.offset); err != nil {
			return err
		}
		seg.offset += int64(cellHeaderSize)
	}

	// write key
	{
		if _, err := seg.file.WriteAt(key, seg.offset); err != nil {
			return err
		}
		seg.offset += int64(len(key))
	}

	// delete from hash map
	ok := seg.hashMap.Delete(key)
	if !ok {
		return ErrKeyNotFound
	}

	return nil
}

func (seg *Segment) Get(key []byte) ([]byte, error) {
	seg.mux.Lock()
	defer seg.mux.Unlock()

	offset, found := seg.hashMap.Get(key)
	if !found {
		return nil, ErrKeyNotFound
	}

	// read header
	var header cellHeader
	{
		headerBytes := header.marshal()
		if _, err := seg.file.ReadAt(headerBytes[:], offset); err != nil {
			return nil, fmt.Errorf("failed to read header at offset %d: %w", offset, err)
		}
		header.unmarshal(headerBytes)
	}

	// read value
	value := make([]byte, header.valueSize)
	{
		offset := offset + int64(cellHeaderSize) + int64(header.keySize)
		if _, err := seg.file.ReadAt(value, offset); err != nil {
			return nil, fmt.Errorf("failed to read value at offset %d: %w", offset, err)
		}
	}

	return value, nil
}

func openFile(dir string, segmentId int) (*os.File, error) {
	filePath := filepath.Join(dir, fmt.Sprintf("segment_%d.data", segmentId))
	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	if _, err := file.Seek(0, io.SeekEnd); err != nil {
		return nil, err
	}
	return file, nil
}

func (seg *Segment) restoreHashMap() error {
	offset := int64(0)
	for {
		currentOffset := offset

		// read header
		var header cellHeader
		{
			headerBytes := header.marshal()
			if _, err := seg.file.ReadAt(headerBytes[:], offset); err != nil {
				if err == io.EOF {
					break
				}
				return fmt.Errorf("failed to read header at offset %d: %w", offset, err)
			}
			header.unmarshal(headerBytes)
			offset += int64(cellHeaderSize)
		}

		// read key
		key := make([]byte, header.keySize)
		{
			if _, err := seg.file.ReadAt(key, offset); err != nil {
				return fmt.Errorf("failed to read key at offset %d: %w", offset, err)
			}
			offset += int64(header.keySize)
		}

		// skip value
		offset += int64(header.valueSize)

		if header.tombstone {
			seg.hashMap.Delete(key)
		} else {
			seg.hashMap.Put(key, currentOffset)
		}
	}

	return nil
}
