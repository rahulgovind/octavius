package main

import (
	"encoding/binary"
	"hash/fnv"
)

func bigToLittle(x uint64) uint64 {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, x)
	return binary.LittleEndian.Uint64(b)
}

func hashInt(s string) int64 {
	h := fnv.New64a()
	h.Write([]byte(s))

	r := int64((bigToLittle(h.Sum64()) >> 2) & 0x3fffffffffffffff)
	return r
}
