package a001simple

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"sync"
)

var ErrNotFound = errors.New("not found")

type DB struct {
	mu   sync.RWMutex
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

// Put writes a key-value pair to the database.
func (db *DB) Put(key, value []byte) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	var buf bytes.Buffer
	if _, err := buf.Write(key); err != nil {
		return err
	}
	if _, err := buf.Write(value); err != nil {
		return err
	}

	// append the length of the key and value at the end
	keyLen := uint32(len(key))
	if err := binary.Write(&buf, binary.LittleEndian, keyLen); err != nil {
		return err
	}
	valueLen := uint32(len(value))
	if err := binary.Write(&buf, binary.LittleEndian, valueLen); err != nil {
		return err
	}

	db.size += int64(buf.Len())

	// move the file pointer to the end of the file
	if _, err := db.file.Seek(0, io.SeekEnd); err != nil {
		return err
	}

	// write the buffer to the file
	if _, err := buf.WriteTo(db.file); err != nil {
		return err
	}

	// flush the buffer to the file
	if err := db.file.Sync(); err != nil {
		return err
	}

	return nil
}

// Get reads a value from the database by its key.
func (db *DB) Get(key []byte) ([]byte, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	// read the last 8 bytes to get the length of the key and value
	var buf [8]byte
	offset := db.size - 8
	for offset >= 0 {
		_, err := db.file.ReadAt(buf[:], offset)
		if err != nil {
			return nil, err
		}

		keyLen := binary.LittleEndian.Uint32(buf[:4])
		valueLen := binary.LittleEndian.Uint32(buf[4:8])

		// read the key at the current offset
		keyBuf := make([]byte, keyLen)
		_, err = db.file.ReadAt(keyBuf, offset-int64(keyLen)-int64(valueLen))
		if err != nil {
			return nil, err
		}

		// if key matches, read the value
		if bytes.Equal(key, keyBuf) {
			value := make([]byte, valueLen)
			_, err := db.file.ReadAt(value, offset-int64(valueLen))
			if err != nil {
				return nil, err
			}
			return value, nil
		}

		offset -= int64(keyLen) + int64(valueLen) + 8
	}

	return nil, ErrNotFound
}
