package a002bitcask

type InMemorySortedMap interface {
	Put(key, value []byte)
	Get(key []byte) ([]byte, bool)
	Has(key []byte) bool
	Len() int
	Delete(key []byte) bool
	Ascend(from []byte, fn func(key, value []byte) bool)
	Descend(from []byte, fn func(key, value []byte) bool)
}

type DB struct {
	dir      string
	segments []Segment
}

func NewDB(dir string) *DB {
	return &DB{
		dir:      dir,
		segments: []Segment{},
	}
}

func (db *DB) Put(key, value []byte) error {
	panic("not implemented")
}

func (db *DB) Get(key []byte) ([]byte, error) {
	panic("not implemented")
}

func (db *DB) Delete(key []byte) error {
	panic("not implemented")
}
