package storage

import (
	"encoding/hex"
	"errors"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"

	ihash "github.com/ajnavarro/super-blockstorage/hash"
)

type ObjectStorage struct {
	path string
}

func NewObjectStorage(path string) *ObjectStorage {
	return &ObjectStorage{path: path}
}

func (s *ObjectStorage) Add(key ihash.Hash, value []byte) error {
	fp := s.filePath(key)
	dir := path.Dir(fp)

	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	return os.WriteFile(fp, value, 0755)
}

func (s *ObjectStorage) Del(key ihash.Hash) error {
	err := os.Remove(s.filePath(key))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}

	return err
}

func (s *ObjectStorage) Get(key ihash.Hash) ([]byte, error) {
	return os.ReadFile(s.filePath(key))
}

func (s *ObjectStorage) Has(key ihash.Hash) (bool, error) {
	_, err := os.Stat(s.filePath(key))
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	} else if err == nil {
		return true, nil
	}

	return false, err
}

func (s *ObjectStorage) DeleteAll() error {
	return os.RemoveAll(s.path)
}

func (s *ObjectStorage) GetAll() (*Iterator, error) {
	return newIterator(s.path)
}

func (s *ObjectStorage) filePath(key ihash.Hash) string {
	name := hex.EncodeToString(key[:])
	return path.Join(s.path, name[0:2], name)
}

type Iterator struct {
	reading []string
}

func newIterator(folder string) (*Iterator, error) {
	var entries []string
	err := filepath.WalkDir(folder, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if d.IsDir() {
			return nil
		}

		entries = append(entries, p)

		return nil
	})

	return &Iterator{reading: entries}, err
}

func (i *Iterator) Next() (ihash.Hash, []byte, error) {
	if len(i.reading) == 0 {
		return ihash.Hash{}, nil, io.EOF
	}

	de, reading := i.reading[0], i.reading[1:]
	i.reading = reading

	pn := path.Base(de)
	key, err := hex.DecodeString(pn)
	if err != nil {
		return ihash.Hash{}, nil, err
	}

	var keyOut ihash.Hash
	copy(keyOut[:], key)

	value, err := os.ReadFile(de)

	return keyOut, value, err
}
