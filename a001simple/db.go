package a001simple

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"sync"
)

var ErrNotFound = errors.New("not found")

type DB struct {
	mu   sync.Mutex
	file *os.File
	size int64
}

func NewDB(f *os.File) *DB {
	info, err := f.Stat()
	if err != nil {
		panic(err)
	}

	return &DB{file: f, size: info.Size()}
}

// 00000000: 7661 6c75 6531 6b65 7931 0600 0000 0400  value1key1......
// 00000010: 0000 00                                  ...

// value, key, metadata の順で入る。
// metadata は、0600 0000 0400 0000 00 の部分だが、
// 0600 はvalue の長さ, 0400 はkey の長さ 00 は削除フラグ

type metadata struct {
	// deleted flag
	tombstone bool
	keyLen    uint32
	valueLen  uint32
}

const tombstoneSize = 1
const keyLenSize = 4
const valueLenSize = 4

const metadataSize = tombstoneSize + keyLenSize + valueLenSize

func (m *metadata) Marshal() ([]byte, error) {
	// TODO: specify the capacity of the buffer
	buf := new(bytes.Buffer)

	if err := binary.Write(buf, binary.LittleEndian, m.valueLen); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, m.keyLen); err != nil {
		return nil, err
	}

	if err := binary.Write(buf, binary.LittleEndian, m.tombstone); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func (m *metadata) Unmarshal(buf []byte) error {
	if len(buf) < tombstoneSize+keyLenSize+valueLenSize {
		return errors.New("invalid buffer size")
	}

	m.tombstone = buf[0] == 1
	m.valueLen = binary.LittleEndian.Uint32(buf[:valueLenSize])
	m.keyLen = binary.LittleEndian.Uint32(buf[valueLenSize : metadataSize-tombstoneSize])

	return nil
}

func (m *metadata) String() string {
	return fmt.Sprintf("tombstone: %t, keyLen: %d, valueLen: %d", m.tombstone, m.keyLen, m.valueLen)
}

// Put writes a key-value pair to the database.
func (db *DB) Put(key, value []byte) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	var buf bytes.Buffer
	if _, err := buf.Write(value); err != nil {
		return err
	}
	if _, err := buf.Write(key); err != nil {
		return err
	}
	meta := metadata{
		tombstone: false,
		keyLen:    uint32(len(key)),
		valueLen:  uint32(len(value)),
	}
	metaBytes, err := meta.Marshal()
	if err != nil {
		return err
	}
	if _, err := buf.Write(metaBytes); err != nil {
		return err
	}

	bytes := buf.Bytes()

	// write the buffer to the file
	if _, err := db.file.Write(bytes); err != nil {
		return err
	}
	db.size += int64(len(bytes))

	return nil
}

// Get reads a value from the database by its key.
func (db *DB) Get(key []byte) ([]byte, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	// read the last 8 bytes to get the length of the key and value
	var buf [metadataSize]byte
	offset := db.size - int64(len(buf))
	for offset >= 0 {
		_, err := db.file.ReadAt(buf[:], offset)
		if err != nil {
			return nil, err
		}

		var meta metadata
		if err := meta.Unmarshal(buf[:]); err != nil {
			return nil, err
		}

		// read the key at the current offset
		keyBuf := make([]byte, meta.keyLen)
		_, err = db.file.ReadAt(keyBuf, offset-int64(meta.keyLen))
		if err != nil {
			return nil, err
		}

		// if key matches, read the value
		if bytes.Equal(key, keyBuf) {
			value := make([]byte, meta.valueLen)
			_, err := db.file.ReadAt(value, offset-int64(meta.keyLen)-int64(meta.valueLen))
			if err != nil {
				return nil, err
			}

			return value, nil
		}

		offset -= int64(meta.keyLen) + int64(meta.valueLen) + metadataSize
	}

	return nil, ErrNotFound
}

// Delete removes a key-value pair from the database.
func (db *DB) Delete(key []byte) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	rec := metadata{
		tombstone: true,
		keyLen:    uint32(len(key)),
		valueLen:  0,
	}

	bytes, err := rec.Marshal()
	if err != nil {
		return err
	}

	if _, err := db.file.Write(bytes); err != nil {
		return err
	}

	db.size += metadataSize

	return nil
}
