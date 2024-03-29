package idx

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"os"

	ihash "github.com/ajnavarro/super-blockstore/hash"
	"github.com/ajnavarro/super-blockstore/iio"
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
		fanoutTable: make([]uint32, fanoutSize),
	}
}

type IndexReader struct {
	fanoutTable   []uint32
	fanoutMapping [256]int

	names     [][]byte
	crcs32    [][]byte
	offsets32 [][]byte
	offsets64 []byte
	sizes     [][]byte
}

func (idx *IndexReader) ReadFrom(r io.Reader) (int64, error) {
	var nOut int64

	flow := []func(io.Reader) (int, error){
		readSignature,
		readVersion,
		idx.readFanout,
		idx.readNames,
		idx.readCRC,
		idx.readSizes,
		idx.readOffsets,
		// TODO footer with checksum
	}

	for _, e := range flow {
		n, err := e(r)
		nOut += int64(n)
		if err != nil {
			return nOut, err
		}
	}

	return nOut, nil
}

func (idx *IndexReader) readFanout(r io.Reader) (int, error) {
	var nOut int
	for k := 0; k < fanoutSize; k++ {
		var v uint32
		if err := binary.Read(r, binary.BigEndian, &v); err != nil {
			return nOut, err
		}

		nOut += 4

		idx.fanoutTable[k] = v
		idx.fanoutMapping[k] = noMapping
	}

	return nOut, nil
}

func (idx *IndexReader) readNames(r io.Reader) (int, error) {
	var nOut int
	for k := 0; k < fanoutSize; k++ {
		var buckets uint32
		if k == 0 {
			buckets = idx.fanoutTable[k]
		} else {
			buckets = idx.fanoutTable[k] - idx.fanoutTable[k-1]
		}

		if buckets == 0 {
			continue
		}

		idx.fanoutMapping[k] = len(idx.names)

		nameLen := int(buckets * ihash.KeySize)
		bin := make([]byte, nameLen)
		n, err := io.ReadFull(r, bin)
		if err != nil {
			return nOut, err
		}

		nOut += n

		idx.names = append(idx.names, bin)
		idx.crcs32 = append(idx.crcs32, make([]byte, buckets*4))
		idx.offsets32 = append(idx.offsets32, make([]byte, buckets*4))
		idx.sizes = append(idx.sizes, make([]byte, buckets*4))
	}

	return nOut, nil
}

func (idx *IndexReader) readCRC(r io.Reader) (int, error) {
	var nOut int
	for k := 0; k < fanoutSize; k++ {
		if pos := idx.fanoutMapping[k]; pos != noMapping {
			n, err := io.ReadFull(r, idx.crcs32[pos])
			if err != nil {
				return nOut, err
			}

			nOut += n
		}
	}

	return nOut, nil
}

func (idx *IndexReader) readOffsets(r io.Reader) (int, error) {
	var nOut int
	var o64cnt int
	for k := 0; k < fanoutSize; k++ {
		if pos := idx.fanoutMapping[k]; pos != noMapping {
			n, err := io.ReadFull(r, idx.offsets32[pos])
			if err != nil {
				return nOut, err
			}

			nOut += n

			for p := 0; p < len(idx.offsets32[pos]); p += 4 {
				if idx.offsets32[pos][p]&(byte(1)<<7) > 0 {
					o64cnt++
				}
			}
		}
	}

	if o64cnt > 0 {
		idx.offsets64 = make([]byte, o64cnt*8)
		n, err := io.ReadFull(r, idx.offsets64)
		if err != nil {
			return nOut, err
		}

		nOut += n
	}

	return nOut, nil
}

func (idx *IndexReader) readSizes(r io.Reader) (int, error) {
	var nOut int
	for k := 0; k < fanoutSize; k++ {
		if pos := idx.fanoutMapping[k]; pos != noMapping {
			n, err := io.ReadFull(r, idx.sizes[pos])
			if err != nil {
				return nOut, err
			}

			nOut += n
		}
	}

	return nOut, nil
}

func (idx *IndexReader) Count() (int64, error) {
	return int64(idx.fanoutTable[len(idx.fanoutTable)-1]), nil
}

func (idx *IndexReader) GetOffset(h ihash.Hash) (int64, error) {
	if len(idx.fanoutMapping) <= int(h[0]) {
		return 0, ErrEntryNotFound
	}

	k := idx.fanoutMapping[h[0]]
	i, ok := idx.findHashIndex(h)
	if !ok {
		return 0, ErrEntryNotFound
	}

	offset := idx.getOffset(k, i)

	return int64(offset), nil
}

const isO64Mask = uint64(1) << 31

func (idx *IndexReader) getOffset(firstLevel, secondLevel int) uint64 {
	offset := secondLevel << 2
	ofs := binary.BigEndian.Uint32(idx.offsets32[firstLevel][offset : offset+4])

	if (uint64(ofs) & isO64Mask) != 0 {
		offset := 8 * (uint64(ofs) & ^isO64Mask)
		n := binary.BigEndian.Uint64(idx.offsets64[offset : offset+8])
		return n
	}

	return uint64(ofs)
}

func (idx *IndexReader) findHashIndex(h ihash.Hash) (int, bool) {
	k := idx.fanoutMapping[h[0]]
	if k == noMapping {
		return 0, false
	}

	if len(idx.names) <= k {
		return 0, false
	}

	data := idx.names[k]
	high := uint64(len(idx.offsets32[k])) >> 2
	if high == 0 {
		return 0, false
	}

	low := uint64(0)
	for {
		mid := (low + high) >> 1
		offset := mid * ihash.KeySize

		cmp := bytes.Compare(h[:], data[offset:offset+ihash.KeySize])
		if cmp < 0 {
			high = mid
		} else if cmp == 0 {
			return int(mid), true
		} else {
			low = mid + 1
		}

		if low >= high {
			break
		}
	}

	return 0, false
}

func (idx *IndexReader) Contains(h ihash.Hash) (bool, error) {
	_, ok := idx.findHashIndex(h)
	return ok, nil
}

func (idx *IndexReader) GetCRC32(h ihash.Hash) (uint32, error) {
	firstLevel := idx.fanoutMapping[h[0]]
	secondLevel, ok := idx.findHashIndex(h)
	if !ok {
		return 0, ErrEntryNotFound
	}

	offset := secondLevel << 2
	return binary.BigEndian.Uint32(idx.crcs32[firstLevel][offset : offset+4]), nil
}

func (idx *IndexReader) GetSize(h ihash.Hash) (uint32, error) {
	firstLevel := idx.fanoutMapping[h[0]]
	secondLevel, ok := idx.findHashIndex(h)
	if !ok {
		return 0, ErrEntryNotFound
	}

	offset := secondLevel << 2
	return binary.BigEndian.Uint32(idx.sizes[firstLevel][offset : offset+4]), nil
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

func readSignature(r io.Reader) (int, error) {
	sig := make([]byte, len(indexSig))
	n, err := io.ReadFull(r, sig)

	if err != nil {
		return n, err
	}

	if !bytes.Equal(indexSig, sig) {
		return n, errors.New("not a valid idx file")
	}

	return n, nil
}

func readVersion(r io.Reader) (int, error) {
	var version uint32

	if err := binary.Read(r, binary.BigEndian, &version); err != nil {
		return 0, err
	}

	if version != indexVersion {
		return 4, errors.New("not a valid idx version")
	}

	return 4, nil
}
