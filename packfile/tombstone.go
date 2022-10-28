package packfile

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"io"
	"os"
	"sort"
)

// TODO add LRU cache
// TODO add binary search on disk file to avoid have all on memory
type Tombstone struct {
	f    *os.File
	w    *bufio.Writer
	keys [][][32]byte
}

func NewTombstonePath(f string) (*Tombstone, error) {
	// TODO load file
	fil, err := os.OpenFile(f, os.O_CREATE|os.O_RDWR, 0755)
	if err != nil {
		return nil, err
	}

	ts := &Tombstone{
		f:    fil,
		w:    bufio.NewWriter(fil),
		keys: make([][][32]byte, 256),
	}

	return ts, ts.load(fil)
}

func (ts *Tombstone) load(f *os.File) error {
	for {
		var k [32]byte
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

func (ts *Tombstone) AddHash(k [32]byte) error {
	_, err := ts.w.Write(k[:])
	if err != nil {
		return err
	}

	if err := ts.w.Flush(); err != nil {
		return err
	}

	ts.keys[k[0]] = append(ts.keys[k[0]], k)

	// TODO maybe short only when needed (Has() method)
	Sort(ts.keys[k[0]])

	return nil
}

func (ts *Tombstone) AddKey(key []byte) error {
	return ts.AddHash(sha256.Sum256(key))
}

func (ts *Tombstone) Has(key []byte) (bool, error) {
	k := sha256.Sum256(key)

	bucket := ts.keys[k[0]]
	bucketLen := len(bucket)

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

	ts.keys = make([][][32]byte, 256)

	ts.w.Reset(ts.f)

	return nil
}

func Sort(e [][32]byte) {
	sort.Slice(e, func(i, j int) bool {
		return bytes.Compare(e[i][:], e[j][:]) < 0
	})
}
