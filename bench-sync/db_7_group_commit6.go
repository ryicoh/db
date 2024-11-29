package benchsync

import "os"

type dbGroupCommit6 struct {
	f *os.File
}

func (db *dbGroupCommit6) Put(key, value []byte) chan<- error {
	errCh := make(chan error, 1)

	return errCh
}
