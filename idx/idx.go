package idx

import (
	"bytes"
	"errors"
	"sort"

	ihash "github.com/ajnavarro/super-blockstore/hash"
)

// Index format:
//
// Signature [3]byte: SPI
// Version uint32: 0
// Fanaout table [256]uint32
// NumElements = fanoutTable[len(fanoutTable)-1]
// List of hashes ordered [ihash.HashSize]byte*NumElements
// CRCs [4]byte*NumElements
// Offsets32 [4]byte*NumElements
// Offsets64 [8]byte*NumElements
// Sizes [4]byte*NumElements
//
// TODO footer with checksums

const fanoutSize = 256
const noMapping = -1

var indexSig []byte = []byte{'S', 'P', 'I'}
var indexVersion uint32 = 0

var ErrEntryNotFound = errors.New("entry not found")

type Entries []*Entry

type Entry struct {
	Key    ihash.Hash
	CRC32  uint32
	Offset uint64
	Size   uint32
}

func SortEntriesByHash(e Entries) {
	sort.Slice(e, func(i, j int) bool {
		return bytes.Compare(e[i].Key[:], e[j].Key[:]) < 0
	})
}

//////////////////////////////////////

type Idx interface {
	GetOffset(key ihash.Hash) (string, int64, error)
	Contains(key ihash.Hash) (bool, error)
	GetSize(key ihash.Hash) (uint32, error)
	DeleteAll(packName string) error
	NewTransaction(packName string) (Transaction, error)
	Close() error

	// TODO iterator
}

type Transaction interface {
	Add(key ihash.Hash, crc32 uint32, pos int64, size uint32) error
	Commit() error
	Discard() error
}
