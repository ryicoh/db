package a002bitcask

import (
	"bytes"

	"github.com/google/btree"
)

type InMemorySortedMap interface {
	Put(key, value []byte)
	Get(key []byte) ([]byte, bool)
	Has(key []byte) bool
	Len() int
	Delete(key []byte) bool
	Ascend(from []byte, fn func(key, value []byte) bool)
	Descend(from []byte, fn func(key, value []byte) bool)
}

type item struct {
	key   []byte
	value []byte
}

type inMemorySortedMap struct {
	tree *btree.BTreeG[*item]
}

var _ InMemorySortedMap = (*inMemorySortedMap)(nil)

func NewInMemorySortedMap() *inMemorySortedMap {
	return &inMemorySortedMap{
		tree: btree.NewG[*item](32, func(a, b *item) bool {
			return bytes.Compare(a.key, b.key) < 0
		}),
	}
}

// Get implements InMemorySortedMap.
func (i *inMemorySortedMap) Get(key []byte) ([]byte, bool) {
	item, ok := i.tree.Get(&item{key: key})
	if !ok {
		return nil, false
	}
	return item.value, true
}

// Has implements InMemorySortedMap.
func (i *inMemorySortedMap) Has(key []byte) bool {
	_, ok := i.Get(key)
	return ok
}

// Len implements InMemorySortedMap.
func (i *inMemorySortedMap) Len() int {
	return int(i.tree.Len())
}

// Put implements InMemorySortedMap.
func (i *inMemorySortedMap) Put(key []byte, value []byte) {
	i.tree.ReplaceOrInsert(&item{key: key, value: value})
}

// Delete implements InMemorySortedMap.
func (i *inMemorySortedMap) Delete(key []byte) bool {
	_, ok := i.tree.Delete(&item{key: key})
	return ok
}

// Ascend implements InMemorySortedMap.
func (i *inMemorySortedMap) Ascend(from []byte, fn func(key []byte, value []byte) bool) {
	i.tree.AscendGreaterOrEqual(&item{key: from}, func(item *item) bool {
		return fn(item.key, item.value)
	})
}

// Descend implements InMemorySortedMap.
func (i *inMemorySortedMap) Descend(from []byte, fn func(key []byte, value []byte) bool) {
	i.tree.DescendLessOrEqual(&item{key: from}, func(item *item) bool {
		return fn(item.key, item.value)
	})
}
