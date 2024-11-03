package a002bitcask

import "encoding/base64"

type inMemoryMap[V any] struct {
	m map[string]V
}

var _ InMemoryMap[any] = (*inMemoryMap[any])(nil)

func newInMemoryMap[V any]() *inMemoryMap[V] {
	return &inMemoryMap[V]{
		m: make(map[string]V),
	}
}

// Get implements InMemoryMap.
func (i *inMemoryMap[V]) Get(key []byte) (V, bool) {
	keyBase64 := i.bytesToBase64(key)
	value, ok := i.m[keyBase64]
	return value, ok
}

// Delete implements InMemoryMap.
func (i *inMemoryMap[V]) Delete(key []byte) bool {
	keyBase64 := i.bytesToBase64(key)
	_, ok := i.m[keyBase64]
	if !ok {
		return false
	}
	delete(i.m, keyBase64)
	return true
}

// Has implements InMemoryMap.
func (i *inMemoryMap[V]) Has(key []byte) bool {
	keyBase64 := i.bytesToBase64(key)
	_, ok := i.m[keyBase64]
	return ok
}

// Len implements InMemoryMap.
func (i *inMemoryMap[V]) Len() int {
	return len(i.m)
}

// Put implements InMemoryMap.
func (i *inMemoryMap[V]) Put(key []byte, value V) {
	keyBase64 := i.bytesToBase64(key)
	i.m[keyBase64] = value
}

func (i *inMemoryMap[V]) bytesToBase64(b []byte) string {
	return base64.StdEncoding.WithPadding(base64.StdPadding).EncodeToString(b)
}

func (i *inMemoryMap[V]) base64ToBytes(s string) ([]byte, error) {
	return base64.StdEncoding.DecodeString(s)
}
