package benchsync

type DB interface {
	Put(key, value []byte) error
	Get(key []byte) ([]byte, error)
}
