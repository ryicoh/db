package a002bitcask

import "unsafe"

type header struct {
}

const headerSize = int(unsafe.Sizeof(header{}))

const offsetSize = int(unsafe.Sizeof(uint32(0)))
