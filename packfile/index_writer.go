package packfile

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"
	"os"
	"sort"

	ihash "github.com/ajnavarro/super-blockstorage/hash"
	"github.com/ajnavarro/super-blockstorage/iio"
)

var _ io.WriterTo = &IndexWriter{}

const fanoutSize = 256

var indexSig []byte = []byte{'S', 'P', 'I'}
var indexVersion uint32 = 0

type IndexWriter struct {
	// TODO sort keys always
	sorted  bool
	entries [fanoutSize]Entries
	count   uint64
	// TODO FOOTER
	//   TODO index checksum
	//   TODO packfile checksum
}

func WriteIndex(i *IndexWriter, path string) error {
	f, err := iio.OpenFile(path, os.O_WRONLY|os.O_TRUNC|os.O_CREATE, 0755)
	if err != nil {
		return err
	}

	_, err = i.WriteTo(f)
	if err != nil {
		return err
	}

	return f.Close()
}

func NewIndexWriter() *IndexWriter {
	return &IndexWriter{}
}

func (idx *IndexWriter) Add(key []byte, pos int64, size int64) {
	k := ihash.SumBytes(key)
	idx.entries[k[0]] = append(idx.entries[k[0]],
		&Entry{Key: k, Offset: pos, Size: size},
	)

	idx.sorted = false
	idx.count++
}

func (idx *IndexWriter) Sort() {
	if idx.sorted {
		return
	}

	for _, es := range idx.entries {
		SortEntriesByHash(es)
	}

	idx.sorted = true
}

func (idx *IndexWriter) GetRaw(keySha256 ihash.Hash) (*Entry, error) {
	// TODO move to a function
	if !idx.sorted {
		// fullscan on bucket
		for _, e := range idx.entries[keySha256[0]] {
			if bytes.Equal(e.Key[:], keySha256[:]) {
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
			return bytes.Compare(keySha256[:], bucket[i].Key[:]) <= 0
		},
	)

	if ePos >= bucketLen {
		return nil, nil
	}

	e := bucket[ePos]
	if !bytes.Equal(e.Key[:], keySha256[:]) {
		return nil, nil
	}

	return e, nil
}

func (idx *IndexWriter) Get(key []byte) (*Entry, error) {
	return idx.GetRaw(ihash.SumBytes(key))
}

func (i *IndexWriter) Count() (int64, error) {
	return int64(i.count), nil
}

func (i *IndexWriter) WriteTo(writer io.Writer) (int64, error) {
	w := bufio.NewWriterSize(writer, 4096*100)

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
			n, err := w.Write(e.Key[:])
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

	// sizes
	for _, es := range i.entries {
		for _, e := range es {
			if err := binary.Write(w, binary.BigEndian, e.Size); err != nil {
				return nOut, err
			}
			nOut += int64(8)
		}
	}

	// TODO CRCs
	// TODO footer

	return nOut, w.Flush()
}
