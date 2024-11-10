package benchsync

import (
	"encoding/binary"
	"os"
)

type db1 struct {
	f *os.File
}

var _ DB = &db1{}

func OpenDB1(fileName string) (*db1, error) {
	f, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	return &db1{f: f}, nil
}

const (
	keyLenSize   = 4
	valueLenSize = 4
)

func (db *db1) Put(key, value []byte) error {
	buf := make([]byte, keyLenSize+len(key)+valueLenSize+len(value))
	keyLen := uint32(len(key))
	{
		binary.BigEndian.PutUint32(buf, keyLen)
		copy(buf[keyLenSize:], key)
	}
	{
		valueOffset := keyLenSize + keyLen
		binary.BigEndian.PutUint32(buf[valueOffset:], uint32(len(value)))
		copy(buf[valueOffset+valueLenSize:], value)
	}

	_, err := db.f.Write(buf)
	if err != nil {
		return err
	}
	return nil
}

func (db *db1) Get(key []byte) ([]byte, error) {
	panic("not implemented")
}

func (db *db1) Sync() error {
	return db.f.Sync()
}

func (db *db1) Close() error {
	return db.f.Close()
}
