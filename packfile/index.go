package packfile

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"sort"

	ihash "github.com/ajnavarro/super-blockstorage/hash"
	"github.com/ajnavarro/super-blockstorage/iio"
)

var _ io.WriterTo = &Index{}
var _ io.ReaderFrom = &Index{}

const fanoutSize = 256

var indexSig []byte = []byte{'S', 'P', 'I'}
var indexVersion uint32 = 0

type Index struct {
	// TODO sort keys always
	sorted  bool
	entries [fanoutSize]Entries
	count   uint64
	// TODO FOOTER
	//   TODO index checksum
	//   TODO packfile checksum
}

func WriteIndexAtomically(i *Index, from, to string) error {
	f, err := iio.OpenFile(from, os.O_WRONLY|os.O_TRUNC|os.O_CREATE, 0755)
	if err != nil {
		return err
	}

	_, err = i.WriteTo(f)
	if err != nil {
		return err
	}

	if err := f.Close(); err != nil {
		return err
	}

	return iio.Rename(from, to)
}

func NewIndex() *Index {
	return &Index{}
}

func NewIndexFromFile(p string) (*Index, error) {
	idx := NewIndex()
	idxf, err := iio.OpenFile(p, os.O_RDONLY, 0755)
	if err != nil {
		return nil, err
	}
	_, err = idx.ReadFrom(idxf)
	if err != nil {
		return nil, err
	}

	return idx, nil
}

func (idx *Index) Add(key []byte, pos int64) {
	k := ihash.SumBytes(key)
	idx.entries[k[0]] = append(idx.entries[k[0]], &Entry{Key: k[:], Offset: pos})

	idx.sorted = false
}

func (idx *Index) Sort() {
	if idx.sorted {
		return
	}

	for _, es := range idx.entries {
		SortEntriesByHash(es)
	}

	idx.sorted = true
}

func (idx *Index) GetRaw(keySha256 ihash.Hash) (*Entry, error) {
	// TODO move to a function
	if !idx.sorted {
		// fullscan on bucket
		for _, e := range idx.entries[keySha256[0]] {
			if bytes.Equal(e.Key, keySha256[:]) {
				return e, nil
			}
		}

		return nil, nil
	}

	// When sorted we can use the fanout table:

	bucket := idx.entries[keySha256[0]]
	bucketLen := len(bucket)

	ePos := sort.Search(
		bucketLen,
		func(i int) bool {
			return bytes.Compare(keySha256[:], bucket[i].Key) <= 0
		},
	)

	if ePos >= bucketLen {
		return nil, nil
	}

	e := bucket[ePos]
	if !bytes.Equal(e.Key, keySha256[:]) {
		return nil, nil
	}

	return e, nil
}

func (idx *Index) Get(key []byte) (*Entry, error) {
	return idx.GetRaw(ihash.SumBytes(key))
}

func (i *Index) Count() (int64, error) {
	return int64(i.count), nil
}

func (i *Index) WriteTo(w io.Writer) (int64, error) {
	var nOut int64

	i.Sort()

	n, err := w.Write(indexSig)
	if err != nil {
		return nOut, err
	}

	nOut += int64(n)

	if err := binary.Write(w, binary.BigEndian, indexVersion); err != nil {
		return nOut, err
	}

	nOut += int64(4)

	var fanout [fanoutSize]uint64

	for b, es := range i.entries {
		for i := int(b); i < fanoutSize; i++ {
			fanout[i] += uint64(len(es))
		}
	}

	// fanout table
	for _, fo := range fanout {
		if err := binary.Write(w, binary.BigEndian, fo); err != nil {
			return nOut, err
		}

		nOut += int64(8)
	}

	// TODO check that last fanout value == len(entries)

	// hashes
	for _, es := range i.entries {
		for _, e := range es {
			n, err := w.Write(e.Key)
			if err != nil {
				return nOut, err
			}
			nOut += int64(n)
		}
	}

	// offsets
	for _, es := range i.entries {
		for _, e := range es {
			if err := binary.Write(w, binary.BigEndian, e.Offset); err != nil {
				return nOut, err
			}
			nOut += int64(8)
		}
	}

	// TODO CRCs
	// TODO footer

	return nOut, nil
}
func (idx *Index) ReadFrom(r io.Reader) (int64, error) {
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

	// discard all fanout values except last one (total number of elements)
	copied, err := io.CopyN(io.Discard, r, int64(8*fanoutSize-8))
	if err != nil {
		return nOut, err
	}

	nOut += copied

	if err := binary.Read(r, binary.BigEndian, &idx.count); err != nil {
		return nOut, err
	}

	nOut += int64(8)

	// read keys and add entries to buckets
	for i := 0; i < int(idx.count); i++ {
		key := make([]byte, ihash.KeySize)
		n, err := io.ReadFull(r, key)
		if err != nil {
			return nOut, err
		}

		nOut += int64(n)

		idx.entries[key[0]] = append(idx.entries[key[0]], &Entry{
			Key: key,
		})
	}

	// read offsets
	for i := 0; i < len(idx.entries); i++ {
		for j := 0; j < len(idx.entries[i]); j++ {
			var offset int64
			if err := binary.Read(r, binary.BigEndian, &offset); err != nil {
				return nOut, err
			}

			nOut += int64(8)

			idx.entries[i][j].Offset = offset
		}
	}

	// TODO crc

	// we supposed that the index is well formed
	idx.sorted = true

	return nOut, nil
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
	Key    []byte
	Offset int64
	// TODO size?
	// TODO CRC
	// TODO indexPos
}

type Entries []*Entry
