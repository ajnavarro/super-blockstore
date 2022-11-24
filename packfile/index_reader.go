package packfile

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"

	ihash "github.com/ajnavarro/super-blockstorage/hash"
	"github.com/ajnavarro/super-blockstorage/iio"
)

var _ io.ReaderFrom = &IndexReader{}

func NewIndexFromFile(p string) (*IndexReader, error) {
	idx := NewIndexReader()
	idxf, err := iio.OpenFile(p, os.O_RDONLY, 0755)
	if err != nil {
		return nil, err
	}
	_, err = idx.ReadFrom(idxf)
	if err != nil {
		return nil, err
	}

	return idx, idxf.Close()
}

func NewIndexReader() *IndexReader {
	return &IndexReader{
		names:   make([][]int64, fanoutSize),
		offsets: make([][]byte, fanoutSize),
		sizes:   make([][]byte, fanoutSize),
	}
}

type IndexReader struct {
	count       int64
	fanoutTable []int64
	names       [][]int64
	offsets     [][]byte
	sizes       [][]byte
}

func (idx *IndexReader) ReadFrom(r io.Reader) (int64, error) {
	var nOut int64

	sig := make([]byte, len(indexSig))

	n, err := io.ReadFull(r, sig)
	if err != nil {
		return nOut, err
	}

	nOut += int64(n)

	if !bytes.Equal(indexSig, sig) {
		return nOut, errors.New("not a valid idx file")
	}

	var version uint32
	if err := binary.Read(r, binary.BigEndian, &version); err != nil {
		return nOut, err
	}

	nOut += int64(4)

	if version != indexVersion {
		return nOut, errors.New("not a valid idx version")
	}

	// fanout

	fanoutRaw := make([]byte, fanoutSize*8)

	n, err = io.ReadFull(r, fanoutRaw)
	if err != nil {
		return nOut, err
	}

	nOut += int64(n)

	frr := bytes.NewBuffer(fanoutRaw)
	for i := 0; i < fanoutSize; i++ {
		var fv int64
		if err := binary.Read(frr, binary.BigEndian, &fv); err != nil {
			return nOut, err
		}

		idx.fanoutTable = append(idx.fanoutTable, fv)
	}

	idx.count = idx.fanoutTable[fanoutSize-1]

	for k := 0; k < fanoutSize; k++ {
		var bucketCount int64

		if k == 0 {
			bucketCount = idx.fanoutTable[k]
		} else {
			bucketCount = idx.fanoutTable[k] - idx.fanoutTable[k-1]
		}

		if bucketCount == 0 {
			continue
		}

		if bucketCount < 0 {
			return nOut, errors.New("negative bucket count")
		}

		nameLen := bucketCount * ihash.KeySize

		raw := make([]byte, nameLen)
		bin := make([]int64, nameLen>>4)

		n, err := io.ReadFull(r, raw)
		if err != nil {
			return nOut, err
		}

		nOut += int64(n)

		for i := 0; i < len(bin); i++ {
			bin[i] = int64(binary.BigEndian.Uint64(raw[i<<4:]))
		}

		idx.names[k] = bin
		idx.offsets[k] = make([]byte, bucketCount*8)
		idx.sizes[k] = make([]byte, bucketCount*8)
	}

	// read offsets
	for k := 0; k < fanoutSize; k++ {
		n, err := io.ReadFull(r, idx.offsets[k])
		if err != nil {
			return nOut, err
		}

		nOut += int64(n)
	}

	// read sizes
	for k := 0; k < fanoutSize; k++ {
		n, err := io.ReadFull(r, idx.sizes[k])
		if err != nil {
			return nOut, err
		}

		nOut += int64(n)
	}

	// // TODO crc

	// // we supposed that the index is well formed
	// idx.sorted = true

	return nOut, nil
}

func (idx *IndexReader) Count() (int64, error) {
	return int64(idx.count), nil
}

func (idx *IndexReader) GetRaw(keySha256 ihash.Hash) (*Entry, error) {
	l1 := keySha256[0]
	data := idx.names[l1]
	high := len(idx.offsets[l1]) >> 4
	if high == 0 {
		return nil, nil
	}

	low := 0

	for {
		mid := (low + high) >> 1
		mid4 := mid << 2

		toCompare := mid4 + mid

		fmt.Println(toCompare)

		if low > high {
			break
		}
	}

	return nil, fmt.Errorf("NOT IMPLEMENTED")
}

// TODO entriesbyoffset
// TODO entriesbyhash

// EntryIter is an iterator that will return the entries in a packfile index.
type EntryIter interface {
	// Next returns the next entry in the packfile index.
	Next() (*Entry, error)
	// Close closes the iterator.
	Close() error
}

func SortEntriesByHash(e Entries) {
	sort.Slice(e, func(i, j int) bool {
		return bytes.Compare(e[i].Key[:], e[j].Key[:]) < 0
	})
}

type Entry struct {
	Key    ihash.Hash
	Offset int64
	Size   int64
	// TODO CRC
	// TODO indexPos
}

type Entries []*Entry
