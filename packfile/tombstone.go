package packfile

import (
	"bufio"
	"bytes"
	"io"
	"os"
	"sort"

	ihash "github.com/ajnavarro/super-blockstorage/hash"
)

// TODO add LRU cache
// TODO add binary search on disk file to avoid have all on memory
type Tombstone struct {
	f *os.File
	w *bufio.Writer

	keys   [][]ihash.Hash
	sorted []bool
}

func NewTombstonePath(f string) (*Tombstone, error) {
	fil, err := os.OpenFile(f, os.O_CREATE|os.O_RDWR, 0755)
	if err != nil {
		return nil, err
	}

	ts := &Tombstone{
		f:      fil,
		w:      bufio.NewWriter(fil),
		keys:   make([][]ihash.Hash, 256),
		sorted: make([]bool, 256),
	}

	return ts, ts.load(fil)
}

func (ts *Tombstone) load(f *os.File) error {
	for {
		var k ihash.Hash
		_, err := io.ReadFull(f, k[:])
		if err == io.EOF {
			break

		}

		if err != nil {
			return err
		}

		if err := ts.AddHash(k); err != nil {
			return err
		}
	}

	return nil
}

// AddHash adds a hash directly to the list.
func (ts *Tombstone) AddHash(k ihash.Hash) error {
	ts.sorted[k[0]] = false

	_, err := ts.w.Write(k[:])
	if err != nil {
		return err
	}

	if err := ts.w.Flush(); err != nil {
		return err
	}

	ts.keys[k[0]] = append(ts.keys[k[0]], k)

	return nil
}

// AddKey adds any key to the deleted list. It will be converted as SHA256
func (ts *Tombstone) AddKey(key []byte) error {
	return ts.AddHash(ihash.SumBytes(key))
}

func (ts *Tombstone) HasHash(k ihash.Hash) (bool, error) {
	bucketLen := len(ts.keys[k[0]])
	if bucketLen == 0 {
		return false, nil
	}

	if !ts.sorted[k[0]] {
		Sort(ts.keys[k[0]])
		ts.sorted[k[0]] = true
	}

	bucket := ts.keys[k[0]]

	ePos := sort.Search(
		bucketLen,
		func(i int) bool {
			return bytes.Compare(k[:], bucket[i][:]) <= 0
		},
	)

	if ePos >= bucketLen {
		return false, nil
	}

	bk := bucket[ePos]
	if !bytes.Equal(bk[:], k[:]) {
		return false, nil
	}

	return true, nil
}

// Has checks if the key is on the list.
func (ts *Tombstone) Has(key []byte) (bool, error) {
	return ts.HasHash(ihash.SumBytes(key))
}

func (ts *Tombstone) Close() error {
	return ts.f.Close()
}

func (ts *Tombstone) Clear() error {
	_, err := ts.f.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}

	if err := ts.f.Truncate(0); err != nil {
		return err
	}

	ts.keys = make([][]ihash.Hash, 256)

	ts.w.Reset(ts.f)

	return nil
}

func Sort(e []ihash.Hash) {
	sort.Slice(e, func(i, j int) bool {
		return bytes.Compare(e[i][:], e[j][:]) < 0
	})
}
